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
 * @file PhysicalLoad2D.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the Load2D SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <fstream>
#include <sstream>
#include <cstdlib>
#define CHUNK_SIZE 10000
using namespace std;

namespace scidb {

class PhysicalLoad2D : public PhysicalOperator {
public:

  PhysicalLoad2D(string const& logicalName,
                   string const& physicalName,
                   Parameters const& parameters,
                   ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  virtual bool changesDistribution(vector<ArrayDesc> const&) const {
    return true;
  }

  virtual RedistributeContext getOutputDistribution(const vector< RedistributeContext>& inputDistributions,
                                                    const vector< ArrayDesc>& inputSchemas) const {
    return RedistributeContext(psLocalInstance);
  }

  shared_ptr<Array> execute(vector< shared_ptr< Array>>& inputArrays, shared_ptr< Query> query) {
    shared_ptr< Array> outputArray(new MemArray(_schema, query));
    shared_ptr< ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    shared_ptr< ChunkIterator> outputChunkIter;
    string::iterator iter;
    Value oneValue;
    Coordinates position(2, 1);
    ifstream fin;
    ofstream log;
    string inputLine;
    string fname = ((shared_ptr< OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
    int64_t n = ((shared_ptr< OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getInt64();
    int64_t d = ((shared_ptr< OperatorParamPhysicalExpression>&)_parameters[2])->getExpression()->evaluate().getInt64();
    int64_t indexStart = 1;
    if (_parameters.size() == 4) {
      indexStart = ((shared_ptr< OperatorParamPhysicalExpression>&)_parameters[3])->getExpression()->evaluate().getInt64();
    }
    int64_t chunkSize = n < CHUNK_SIZE ? n : CHUNK_SIZE;
    double *buf = new double[d];
    int64_t totalChunks = (n-1) / chunkSize + 1;
    int64_t chunksPerInstance = (totalChunks-1) / query->getInstancesCount() + 1;
    int64_t myStartChunkId = query->getInstanceID() * chunksPerInstance;
    int64_t myEndChunkId = myStartChunkId + chunksPerInstance - 1;
    int64_t currChunkId, currRowId, currColId;
    Value valueToWrite;
    fin.open(fname.c_str(), ios::in | ios::binary);
    if (query->getInstanceID() == query->getInstancesCount() - 1) {
      myEndChunkId += totalChunks - myEndChunkId - 1;
    }

    #ifdef DEBUG
      stringstream ss;
      ss << getenv("HOME") << "/load2d-instance-" << query->getInstanceID() << ".log";
      log.open(ss.str().c_str(), ios::out);
      log << "File name is " << fname << endl;
      log << "n = " << n << endl << "d = " << d << endl;
      log << "indexStart = " << indexStart << endl;
      log << "totalChunks = " << totalChunks << endl;
      log << "chunksPerInstance = " << chunksPerInstance << endl;
      log << "I am instance " << query->getInstanceID() << ", there are "
          << query->getInstancesCount() << " instances." << endl;
      log << "myStartChunkId = " << myStartChunkId << endl;
      log << "myEndChunkId = " << myEndChunkId << endl;
    #endif

    if (myStartChunkId >= totalChunks) {
      #ifdef DEBUG
        log << "Nothing to be done on this instance, return." << endl;
        fin.close();
        log.close();
      #endif
      delete[] buf;
      return outputArray;
    }

    fin.seekg(0, ios::end);
    int64_t flen = fin.tellg();
    int64_t offset = myStartChunkId * chunkSize * d * sizeof(double);
    offset = offset % flen;
    int64_t readLen = d * sizeof(double);
    fin.seekg(offset, ios::beg);
    #ifdef DEBUG
      log << "Seek to " << offset << endl;
      bool rollback = false;
    #endif
    for (currChunkId=myStartChunkId; currChunkId<=myEndChunkId; currChunkId++) {
      position[0] = currChunkId * chunkSize + indexStart;
      position[1] = indexStart;
      outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
      #ifdef DEBUG
        log << "Create chunk at (" << position[0] << ", " << position[1] << ")" << endl;
      #endif
      for (currRowId=0; currRowId<chunkSize && position[0] < n+indexStart; currRowId++, position[0]++) {
        position[1] = indexStart;
        fin.read((char*)buf, readLen);
        if (fin.gcount() != readLen) {
          #ifdef DEBUG
            log << "Rolling back to the beginning of the file" << endl;
            rollback = true;
          #endif
          currRowId--;
          position[0]--;
          fin.clear();
          fin.seekg(0, ios::beg);
          continue;
        }
        #ifdef DEBUG
          if (rollback) {
            log << "Read data after rolling back to the beginning successfully" << endl;
          }
        #endif
        for (currColId=0; currColId<d; currColId++, position[1]++) {
          #ifdef DEBUG
            if (rollback) {
              log << "Write " << buf[currColId] << " to (" << position[0] << ", " << position[1] << ")" << endl;
            }
          #endif
          outputChunkIter->setPosition(position);
          valueToWrite.setDouble(buf[currColId]);
          outputChunkIter->writeItem(valueToWrite);
        }
        #ifdef DEBUG
          if (rollback) {
            rollback = false;
          }
        #endif
      }
      outputChunkIter->flush();
    }
    #ifdef DEBUG
      log << "Cleanning up..." << endl;
      log.close();
    #endif
    fin.close();
    delete[] buf;
    return outputArray;
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalLoad2D, "load2d", "PhysicalLoad2D");

} //namespace scidb
