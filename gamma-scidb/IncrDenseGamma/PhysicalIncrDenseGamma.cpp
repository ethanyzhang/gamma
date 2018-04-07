/*
 * Copyright (C) 2013-2017 Yiqun Zhang <zhangyiqun9164@gmail.com>
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
 * @file PhysicalIncrDenseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the IncrDenseGamma SciDB operator.
 */

// This operator is not optimized for entries with value zero.

#include <query/Operator.h>
#include <util/Network.h>
#define D_MAX 2000

using namespace std;
// We now use fix-sized static array to store the Gammma matrix. It's also OK if we use dynamic array.
// Using C++ vector will be slow.
namespace scidb {

class PhysicalIncrDenseGamma : public PhysicalOperator {
public:
  
  PhysicalIncrDenseGamma(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  // The use of static arrays may improve the performance a little bit.
  double z_i[D_MAX];
  double Gamma[D_MAX][D_MAX];

  shared_ptr<Array> writeGamma(size_t d, shared_ptr<Query> query) {
    // Output array and its iterator for all the chunks inside that array.
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);

    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(2, 1);
    // The output array has only one chunk.
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

  shared_ptr< Array > execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query) {
    int64_t chunkSerialNumberToContinue = ((shared_ptr< OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();

    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<Array> inputArray = inputArrays[0];
    ArrayDesc inputSchema = inputArray->getArrayDesc();

    // Get descriptor of two dimensions d and n.
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    size_t n = dimsN.getCurrEnd() - dimsN.getCurrStart() + 1;
    // Note: the input data set should have d+1 dimensions (including Y)
    size_t d = dimsD.getCurrEnd() - dimsD.getCurrStart();
    size_t nStart = dimsN.getCurrStart();
    size_t dStart = dimsD.getCurrStart(); 

    // Get chunk size of n.
    size_t nChunkSize = dimsN.getChunkInterval();

    // Helps to accumulate the n and L.
    z_i[0] = 1.0;

    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    Coordinates chunkPosition;

    size_t i, j, k, m;
    int64_t currChunkId = 0;
    while (! inputArrayIter->end() && currChunkId != chunkSerialNumberToContinue) {
      currChunkId++;
      ++(*inputArrayIter);
    }
    if (inputArrayIter->end()) {
      return outputArray;
    }

    // compute on one chunk
    shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
    chunkPosition = inputArrayIter->getPosition();
    for(i=chunkPosition[0]; i<chunkPosition[0] + nChunkSize; i++) {
      // In case the chunk is partially filled.
      if(i == n + nStart) {
        break;
      }
      for(j=chunkPosition[1], m=1; j<=chunkPosition[1]+d; j++, m++) {
        // In case the chunk is partially filled.
        if(j == d + 1 + dStart) {
          break;
        }
        z_i[m] = chunkIter->getItem().getDouble();
        ++(*chunkIter);
      }
      for(k=0; k<=d+1; ++k) {
      // This operator is not optimized for entries with value zero.
      // TODO: should use fabs(z_i[k]) < 10e-6
//          if(z_i[k] == 0.0) {
//            continue;
//          }
        for(m=0; m<=k; ++m) {
          Gamma[k][m] += z_i[k]*z_i[m];
        }
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
        for(size_t i=0; i<d+2; ++i) {
          for(size_t j=0; j<=i; ++j) {
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
          for(size_t i=0; i<d+2; ++i) {
            for(size_t j=0; j<=i; ++j) {
              Gamma[i][j] += *Gammabuf;
              ++Gammabuf;
            }
          }
        }
      } // end if getInstanceID() != 0
    } //end if InstancesCount() > 1

    return writeGamma(d, query);
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalIncrDenseGamma, "IncrDenseGamma", "PhysicalIncrDenseGamma");

} //namespace scidb
