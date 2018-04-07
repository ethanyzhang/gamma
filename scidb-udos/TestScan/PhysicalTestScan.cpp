/*
 * Copyright (C) 2013-2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
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
 * @file PhysicalTestScan.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the TestScan SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
using namespace std;

namespace scidb {

class PhysicalTestScan : public PhysicalOperator {
public:
  
  PhysicalTestScan(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  shared_ptr<Array> execute(vector<shared_ptr<Array>>& inputArrays, shared_ptr<Query> query) {
    bool memTransport = ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getBool();
    shared_ptr<Array> inputArray = inputArrays[0];
    ArrayDesc inputSchema = inputArray->getArrayDesc();
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    size_t nChunkSize = dimsN.getChunkInterval();
    size_t dChunkSize = dimsD.getChunkInterval();
    double* memchunkStorage = new double[nChunkSize*dChunkSize];
    double** memchunk = new double*[nChunkSize];
    for (size_t i=0; i<nChunkSize; i++) {
      memchunk[i] = &memchunkStorage[i*dChunkSize];
    }

    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    Coordinates currPos(2, 1);
    Coordinates chunkPos(2, 1);
    Value val;

    while ( ! inputArrayIter->end()) {
      shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
      chunkPos = chunkIter->getPosition();
      shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(chunkPos).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
      while ( ! chunkIter->end()) {
        currPos = chunkIter->getPosition();
        if (memTransport) {
          memchunk[currPos[0]-chunkPos[0]][currPos[1]-chunkPos[1]] = chunkIter->getItem().getDouble();
        }
        else {
          outputChunkIter->setPosition(currPos);
          outputChunkIter->writeItem(chunkIter->getItem());
        }
        ++(*chunkIter);
      }
      if (memTransport) {
        currPos[0] = chunkPos[0];
        for (size_t i=0; i<nChunkSize; i++, currPos[0]++) {
          currPos[1] = chunkPos[1];
          for (size_t j=0; j<dChunkSize; j++, currPos[1]++) {
            outputChunkIter->setPosition(currPos);
            val.setDouble(memchunk[i][j]);
            outputChunkIter->writeItem(val);
          }
        }
      } // end if
      outputChunkIter->flush();
      ++(*inputArrayIter);
    }
    delete[] memchunkStorage;
    delete[] memchunk;
    return outputArray;
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalTestScan, "TestScan", "PhysicalTestScan");

} //namespace scidb
