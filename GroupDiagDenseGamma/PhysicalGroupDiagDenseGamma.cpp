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
 * @file PhysicalGroupDiagDenseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the GroupDiagDenseGamma SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <map>
#include <sstream>
#define D_MAX 2000

using namespace std;
// We now use fix-sized static array to store the Gammma matrix. It's also OK if we use dynamic array.
// Using C++ vector will be slow.
namespace scidb {

class PhysicalGroupDiagDenseGamma : public PhysicalOperator {
public:
  // Because we are using the i, j, v schema, all data must have same data type.
  // Therefore we set N also as double.
  struct NLQ {
    int64_t groupId;
    double N;
    double L[D_MAX]; // linear sum
    double Q[D_MAX]; // quadratic sum
    NLQ() {
      int i;
      N = 0;
      groupId = 0;
      for(i=0; i<D_MAX; i++) {
        L[i] = Q[i] = 0;
      }
    }
  };

  PhysicalGroupDiagDenseGamma(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  map<int64_t, struct NLQ> nlq;

  shared_ptr<Array> writeGamma(Coordinate k, size_t d, shared_ptr<Query> query) {
    // Output array and its iterator for all the chunks inside that array.
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    Coordinates position(2, 1);
    shared_ptr<ChunkIterator> outputChunkIter;
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);

    size_t i;
    Value valGamma;
    map<int64_t, struct NLQ>::iterator it;
    for(it = nlq.begin(); it != nlq.end(); it++) {
      if(it->first > k) {
        break;
      }
      position[0] = it->first;
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
    shared_ptr<Array> groupArray = inputArrays[1];
    ArrayDesc inputSchema = inputArray->getArrayDesc();
    //ArrayDesc groupSchema = groupArray->getArrayDesc();
    // Get descriptor of two dimensions d and n.
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    size_t n = dimsN.getCurrLength();
    Coordinate k = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();
    /**
     * Note: the input data set should have d+1 dimensions (including Y)
     * For k-means this is not true (data set doesn't have target cluster ID), 
     * but in practise this d setting doesn't change the behavior of the operator.
     */
    size_t d = dimsD.getCurrLength() - 1; 
    size_t nStart = dimsN.getCurrStart();
    size_t dStart = dimsD.getCurrStart(); 
    // Get chunk size of n.
    size_t nChunkSize = dimsN.getChunkInterval();
    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    shared_ptr<ConstArrayIterator> groupArrayIter = groupArray->getConstIterator(0);
    Coordinates inputChunkPosition;
    size_t i, j, m;
    double value;
    map<int64_t, struct NLQ>::iterator it;

    shared_ptr<ConstChunkIterator> groupChunkIter;
    while( ! (inputArrayIter->end() || groupArrayIter->end()) ) {
      shared_ptr<ConstChunkIterator> inputChunkIter = inputArrayIter->getChunk().getConstIterator();
      if( !groupChunkIter ) {
        groupChunkIter = groupArrayIter->getChunk().getConstIterator();
      }
      inputChunkPosition = inputArrayIter->getPosition();
      // nChunkSize rows
      for(i=inputChunkPosition[0]; i<inputChunkPosition[0] + nChunkSize; i++) {
        // in case the chunk is partially filled.
        if(i == n + nStart) {
          break;
        }
        int64_t groupId = groupChunkIter->getItem().getInt64();
        it = nlq.find(groupId);
        if(it == nlq.end()) { // target group does not exist in the hash map.
          nlq[groupId].groupId = groupId;
          nlq[groupId].N = 1;
        }
        else { // target group already exists
          it->second.N = it->second.N + 1;
        }
        for(j=inputChunkPosition[1], m=1; j<=inputChunkPosition[1]+d; j++, m++) {
          if(j == d + 1 + dStart) {
            break;
          }
          value = inputChunkIter->getItem().getDouble();
          nlq[groupId].L[m] += value;
          nlq[groupId].Q[m] += value * value;
          ++(*inputChunkIter);
        }
        ++(*groupChunkIter);
        if( groupChunkIter->end() ) {
          ++(*groupArrayIter);
          groupChunkIter.reset();
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
    return writeGamma(k, d, query);
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupDiagDenseGamma, "GroupDiagDenseGamma", "PhysicalGroupDiagDenseGamma");

} //namespace scidb
