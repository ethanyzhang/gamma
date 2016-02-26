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
 * @file PhysicalGroupDiagDenseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the GroupDiagDenseGamma SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <map>
#define D_MAX 2000

using namespace std;
// We now use fix-sized static array to store the Gammma matrix. It's also OK if we use dynamic array.
// Using C++ vector will be slow.
namespace scidb {

struct NLQ {
  double groupId;
  double N;
  double L[D_MAX];
  double Q[D_MAX];
  NLQ() {
    groupId = 0.0;
    N = 0.0;
    size_t i;
    for(i=0; i<D_MAX; i++) {
      L[i] = Q[i] = 0.0;
    }
  }
};

class PhysicalGroupDiagDenseGamma : public PhysicalOperator {
private:
  map<double, NLQ> nlq;
  Coordinate k;
  size_t d;

public:
  PhysicalGroupDiagDenseGamma(string const& logicalName,
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

    map<double, struct NLQ>::iterator it;

    for(it = nlq.begin(), i=1; it != nlq.end() && i<=k; it++, i++) {
      position[0] = i;
      position[1] = 1;
      valGamma.setDouble(it->second.N);
      outputChunkIter->setPosition(position);
      outputChunkIter->writeItem(valGamma);

      for(i=1; i<=d+1; i++) {
        position[1] = i+1;
        valGamma.setDouble(it->second.L[i]);
        outputChunkIter->setPosition(position);
        outputChunkIter->writeItem(valGamma);
      }
      for(i=1; i<=d+1; i++) {
        position[1] = i+d+2;
        valGamma.setDouble(it->second.Q[i]);
        outputChunkIter->setPosition(position);
        outputChunkIter->writeItem(valGamma);
      }
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
    d = dimsD.getCurrEnd() - dimsD.getCurrStart();
    size_t nStart = dimsN.getCurrStart();
    size_t dStart = dimsD.getCurrStart();
    // Get chunk size of n.
    size_t nChunkSize = dimsN.getChunkInterval();
    k = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();

    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    Coordinates chunkPosition;
    size_t i, j, m;
    double value;
    NLQ tmp;
    map<double, struct NLQ>::iterator it;

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
          tmp.L[m] = value;
          tmp.Q[m] = value * value;
          ++(*chunkIter);
        }
        double Y = tmp.L[d+1];
        it = nlq.find(Y);
        if (it == nlq.end()) {
          nlq[Y].N = 1;
          nlq[Y].groupId = Y;
        }
        else {
          nlq[Y].N++;
        }
        for (i=1; i<=d+1; i++) {
          nlq[Y].L[i] += tmp.L[i];
          nlq[Y].Q[i] += tmp.Q[i];
        }
      }
      ++(*inputArrayIter);
    }

    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * COORDINATOR_INSTANCE if instance execute this query itself.
     */
    size_t localClassCount = nlq.size();
    if(query->getInstancesCount() > 1) {
      if(query->getInstanceID() != 0) {
        // I am not the coordinator, I should send my Gamma matrix out.
        shared_ptr <SharedBuffer> buf ( new MemoryBuffer(NULL, sizeof(struct NLQ) * localClassCount ));
        struct NLQ *NLQbuf = static_cast<struct NLQ*> (buf->getData());
        for(it = nlq.begin(); it != nlq.end(); it++) {
          *NLQbuf = it->second;
          ++NLQbuf;
        }
        BufSend(0, buf, query);
        return outputArray;
      }
      else {
        // I am the coordinator, I should collect Gamma matrix from workers.
        for(InstanceID l = 1; l<query->getInstancesCount(); ++l) {
          shared_ptr<SharedBuffer> buf = BufReceive(l, query);
          if(! buf) {
            continue;
          }
          size_t remoteClassCount = buf->getSize() / sizeof(struct NLQ);
          struct NLQ* NLQbuf = static_cast<struct NLQ*> (buf->getData());
          for(size_t i=0; i<remoteClassCount; ++i) {
            it = nlq.find(NLQbuf->groupId);
            if( it == nlq.end() ) {
              nlq[NLQbuf->groupId] = *NLQbuf;
            }
            else {
              it->second.N += NLQbuf->N;
              for(size_t j=1; j<=d+1; ++j) {
                it->second.L[j] += NLQbuf->L[j];
                it->second.Q[j] += NLQbuf->Q[j];
              }
            }
            ++NLQbuf;
          }

        }
      }// end if getInstanceID() != 0
    }//end if InstancesCount() > 1

    return writeGamma(query);
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupDiagDenseGamma, "GroupDiagDenseGamma", "PhysicalGroupDiagDenseGamma");

} //namespace scidb
