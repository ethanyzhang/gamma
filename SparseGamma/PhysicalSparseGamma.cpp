/*
 * Copyright (C) 2015 Yiqun Zhang <zhangyiqun9164@gmail.com>
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * This Gamma operator is proposed in the paper:
 * The Gamma Operator for Big Data Summarization on an Array DBMS
 * Carlos Ordonez, Yiqun Zhang, Wellington Cabrera 
 * Journal of Machine Learning Research (JMLR): Workshop and Conference Proceedings (BigMine 2014) 
 *
 * Please cite the paper above if you need to use this code in your research work.
 */

/*
 * @file PhysicalSparseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the SparseGamma SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <string.h>
#define D_MAX 2000

using namespace std;
// We now use fix-sized static array to store the Gammma matrix. It's also OK if we use dynamic array.
// Using C++ vector will be slow.
namespace scidb {

class PhysicalSparseGamma : public PhysicalOperator {
public:

  PhysicalSparseGamma(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  // The use of static arrays may improve the performance for a little bit.
  // We use 2-D array for Gamma, we tried 1-D layout before, but experiments 
  // showed that the 1-D layout cannot bring any performance improvement.
  // Therefore we use this more intuitive 2-D layout.
  size_t z_count;
  Coordinate z_index[D_MAX];
  double z_value[D_MAX];
  double Gamma[D_MAX][D_MAX];

  shared_ptr<Array> writeGamma(size_t d, shared_ptr<Query> query) {
    // Output array and its iterator for all the chunks inside that array.
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);

    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(2, 1);
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);

    size_t i, j;
    Value valGamma;
    double value;

    for(i=0; i<d+2; i++) {
      position[0] = i+1;
      for(j=0; j<d+2; j++) {
        if(i>=j) {
          value = Gamma[i][j];
        }
        else {
          value = Gamma[j][i];
        }
        position[1] = j+1;
        outputChunkIter->setPosition(position);
        valGamma.setDouble(value);
        outputChunkIter->writeItem(valGamma);
      }
    }
    outputChunkIter->flush();
    return outputArray;
  }

  shared_ptr<Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
  {

    // Get the input array, its schema and iterator for chunks. 
    shared_ptr<Array> inputArray = inputArrays[0];
    ArrayDesc inputSchema = inputArray->getArrayDesc();
    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);

    // Get descriptor of two dimensions d and n.
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];

    // Note: the input data set should have d+1 dimensions 
    // (including Y in the last place)
    size_t d = dimsD.getCurrLength() - 1;
    size_t dStart = dimsD.getCurrStart(); 

    // Helps to accumulate the n and L.
    z_count = 1; // # non-zero entries

    Coordinates cellPosition;
    Coordinate currRow = -1; // -1 indicates uninitialized.

    size_t i, j;

    // For each chunk in the input array.
    while(! inputArrayIter->end() ) {
      shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
      // For each cell in the current chunk.
      // This will skip the empty cells.
      while(! chunkIter->end() ) {
        cellPosition = chunkIter->getPosition();
        // Now comes to a new row.
        if(currRow != cellPosition[0]) {
          if(currRow != -1) {
            for(i=0; i<z_count; i++) {
              for(j=0; j<=i; j++) {
                Gamma[ z_index[i] ][ z_index[j] ] += z_value[i] * z_value[j];
              }
            }
          }
          currRow = cellPosition[0];
          z_count = 1;
          z_index[0] = 0; 
          z_value[0] = 1;
        }
        z_index[z_count] = cellPosition[1] - dStart + 1;
        z_value[z_count] = chunkIter->getItem().getDouble();
        ++z_count;
        ++(*chunkIter);
      }
      ++(*inputArrayIter);
    }
    for(i=0; i<z_count; i++) {
      for(j=0; j<=i; j++) {
        Gamma[ z_index[i] ][ z_index[j] ] += z_value[i] * z_value[j];
      }
    }
    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * COORDINATOR_INSTANCE if instance execute this query itself.
     */
    if(query->getInstancesCount() > 1) {
      if(query->getInstanceID() != 0) {
        // I am not the coordinator, I should send my Gamma matrix out.
        shared_ptr <SharedBuffer> buf ( new MemoryBuffer(NULL, sizeof(double) * (d+3) * (d+2) / 2) );
        double *Gammabuf = static_cast<double*> (buf->getData());
        shared_ptr<Array> outputArray(new MemArray(_schema, query));
        for(i=0; i<d+2; i++) {
          for(j=0; j<=i; j++) {
            *Gammabuf = Gamma[i][j];
            ++Gammabuf;
          }
        }
        BufSend(0, buf, query);
        return outputArray;
      }
      else {
        // I am the coordinator, I should collect Gamma matrix from workers.
        for(InstanceID l = 1; l<query->getInstancesCount(); ++l) {
          shared_ptr<SharedBuffer> buf = BufReceive(l, query);
          double *Gammabuf = static_cast<double*> (buf->getData());
          for(i=0; i<d+2; i++) {
            for(j=0; j<=i; j++) {
              Gamma[i][j] += *Gammabuf;
              ++Gammabuf;
            }
          }
        }
      }// end if getInstanceID() != 0
    }//end if InstancesCount() > 1
    return writeGamma(d, query);
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSparseGamma, "SparseGamma", "PhysicalSparseGamma");

} //namespace scidb
