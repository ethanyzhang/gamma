/*
 * Copyright (C) 2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
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
 * @file PhysicalDiagDenseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the DiagDenseGamma SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#define D_MAX 2000

using namespace std;
// We now use fix-sized static array to store the Gammma matrix. It's also OK if we use dynamic array.
// Using C++ vector will be slow.
namespace scidb {

struct NLQ {
  double N;
  size_t d;
  double L[D_MAX];
  double Q[D_MAX];
  NLQ() {
    size_t i;
    N = 0.0;
    d = 0;
    for(i=0; i<D_MAX; i++) {
      L[i] = Q[i] = 0.0;
    }
  }
};

class PhysicalDiagDenseGamma : public PhysicalOperator {
private:
  NLQ nlq;

public:
  PhysicalDiagDenseGamma(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  shared_ptr<Array> writeGamma(shared_ptr<Query> query) {
    // Output array and its iterator for all the chunks inside that array.
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);

    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(1, 1);
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);

    size_t i;
    Value valGamma;

    valGamma.setDouble(nlq.N);
    outputChunkIter->setPosition(position);
    outputChunkIter->writeItem(valGamma);

    for(i=1; i<=nlq.d+1; i++) {
      position[0] = i+1;
      valGamma.setDouble(nlq.L[i]);
      outputChunkIter->setPosition(position);
      outputChunkIter->writeItem(valGamma);
    }
    for(i=1; i<=nlq.d+1; i++) {
      position[0] = i+nlq.d+2;
      valGamma.setDouble(nlq.Q[i]);
      outputChunkIter->setPosition(position);
      outputChunkIter->writeItem(valGamma);
    }
    outputChunkIter->flush();
    return outputArray;
  }

  shared_ptr< Array > execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
  {
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<Array> inputArray = inputArrays[0];
    ArrayDesc inputSchema = inputArray->getArrayDesc();

    // Get descriptor of two dimensions d and n.
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    size_t n = dimsN.getCurrEnd() - dimsN.getCurrStart() + 1;
    // Note: the input data set should have d+1 dimensions (including Y)
    size_t d = dimsD.getCurrEnd() - dimsD.getCurrStart();
    nlq.N = n;
    nlq.d = d;
    size_t nStart = dimsN.getCurrStart();
    size_t dStart = dimsD.getCurrStart(); 

    // Get chunk size of n.
    size_t nChunkSize = dimsN.getChunkInterval();

    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    Coordinates chunkPosition;
    size_t i, j, m;
    double value;
    while(! inputArrayIter->end() ) {
      shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
      chunkPosition = inputArrayIter->getPosition();
      for(i=chunkPosition[0]; i<chunkPosition[0] + nChunkSize; i++) {
        if(i == n + nStart) {
          break;
        }
        for(j=chunkPosition[1], m=1; j<=chunkPosition[1]+d; j++, m++) {
          if(j == d + 1 + dStart) {
            break;
          }
          value = chunkIter->getItem().getDouble();
          nlq.L[m] += value;
          nlq.Q[m] += value * value;
          ++(*chunkIter);
        }
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
        for(size_t i=1; i<=d+1; ++i) {
          *Gammabuf = nlq.L[i];
          ++Gammabuf;
        }
        for(size_t i=1; i<=d+1; ++i) {
          *Gammabuf = nlq.Q[i];
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
          for(size_t i=1; i<=d+1; ++i) {
            nlq.L[i] += *Gammabuf;
            ++Gammabuf;
          }
          for(size_t i=1; i<=d+1; ++i) {
            nlq.Q[i] += *Gammabuf;
            ++Gammabuf;
          }
        }
      }// end if getInstanceID() != 0
    }//end if InstancesCount() > 1

    return writeGamma(query);
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalDiagDenseGamma, "DiagDenseGamma", "PhysicalDiagDenseGamma");

} //namespace scidb
