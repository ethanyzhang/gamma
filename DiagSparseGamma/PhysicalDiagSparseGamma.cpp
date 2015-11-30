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
 * @file PhysicalDiagSparseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the DiagSparseGamma SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#define D_MAX 2000

using namespace std;
// We now use fix-sized static array to store the Gammma matrix. It's also OK if we use dynamic array.
// Using C++ vector will be slow.
namespace scidb {

class PhysicalDiagSparseGamma : public PhysicalOperator {
public:

  PhysicalDiagSparseGamma(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  // The use of static arrays may improve the performance a little bit.
  double L[D_MAX];
  double Q[D_MAX];

  void zeroArray() {
    int i;
    for(i=0; i<D_MAX; i++) {
      L[i] = Q[i] = 0.0;
    }
  }

  shared_ptr<Array> writeGamma(size_t n, size_t d, shared_ptr<Query> query) {
    // Output array and its iterator for all the chunks inside that array.
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);

    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(1, 1);
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);

    size_t i;
    Value valGamma;

    valGamma.setDouble(n);
    outputChunkIter->setPosition(position);
    outputChunkIter->writeItem(valGamma);

    for(i=1; i<=d+1; i++) {
      position[0] = i+1;
      valGamma.setDouble(L[i]);
      outputChunkIter->setPosition(position);
      outputChunkIter->writeItem(valGamma);
    }
    for(i=1; i<=d+1; i++) {
      position[0] = i+d+2;
      valGamma.setDouble(Q[i]);
      outputChunkIter->setPosition(position);
      outputChunkIter->writeItem(valGamma);
    }
    outputChunkIter->flush();
    return outputArray;
  }

  shared_ptr< Array > execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query) {
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<Array> inputArray = inputArrays[0];
    ArrayDesc inputSchema = inputArray->getArrayDesc();

    // Get descriptor of two dimensions d and n.
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    size_t n = dimsN.getCurrLength();
    // Note: the input data set should have d+1 dimensions (including Y)
    size_t d = dimsD.getCurrLength() - 1;

    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    Coordinates cellPosition;

    size_t i;
    zeroArray();
    double value;
    while(! inputArrayIter->end() ) {
      shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
      // For each cell in the current chunk.
      // This will skip the empty cells.
      while(! chunkIter->end() ) {
        cellPosition = chunkIter->getPosition();
        value = chunkIter->getItem().getDouble();
        L[ cellPosition[1] ] += value;
        Q[ cellPosition[1] ] += value * value;
        ++(*chunkIter);
      }
      ++(*inputArrayIter);
    }

    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * COORDINATOR_INSTANCE if instance execute this query itself.
     */
    if(query->getInstancesCount() > 1) {
      if(query->getInstanceID() != 0) {
        // I am not the coordinator, I should send my Gamma matrix out.
        shared_ptr <SharedBuffer> buf ( new MemoryBuffer(NULL, sizeof(double) * (d*2+2) ));
        double *Gammabuf = static_cast<double*> (buf->getData());
        for(i=1; i<=d+1; ++i) {
          *Gammabuf = L[i];
          ++Gammabuf;
        }
        for(i=1; i<=d+1; ++i) {
          *Gammabuf = Q[i];
          ++Gammabuf;
        }
        BufSend(0, buf, query);
        return outputArray;
      }
      else {
        // I am the coordinator, I should collect Gamma matrix from workers.
        for(InstanceID l = 1; l<query->getInstancesCount(); ++l) {
          shared_ptr<SharedBuffer> buf = BufReceive(l, query);
          double *Gammabuf = static_cast<double*> (buf->getData());
          for(i=1; i<=d+1; ++i) {
            L[i] += *Gammabuf;
            ++Gammabuf;
          }
          for(i=1; i<=d+1; ++i) {
            Q[i] += *Gammabuf;
            ++Gammabuf;
          }
        }
      }// end if getInstanceID() != 0
    }//end if InstancesCount() > 1

    return writeGamma(n, d, query);
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalDiagSparseGamma, "DiagSparseGamma", "PhysicalDiagSparseGamma");

} //namespace scidb
