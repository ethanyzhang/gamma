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
 * @file PhysicalGPUDenseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the GPUDenseGamma SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <fstream>    // Needed for log file.
#include <sstream>
#include <string>
using namespace std;

void computeGamma(double *memChunk, double *Gamma, size_t d, size_t nChunkSize);

namespace scidb {
class PhysicalGPUDenseGamma : public PhysicalOperator {
public:
  
  PhysicalGPUDenseGamma(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  size_t d, nChunkSize, GammaLen;
  double *memChunkStorage;
  double *GammaStorage;
  double **Gamma;
  double **memChunk;
  ofstream log;

  shared_ptr<Array> execute(vector<shared_ptr<Array>>& inputArrays, shared_ptr<Query> query) {
    shared_ptr<Array> inputArray = inputArrays[0];
    initOperator(inputArray, query);

    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
    while ( ! inputArrayIter->end()) {
      shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
      copyChunkToMemory(chunkIter);
      #ifdef DEBUG
      log << "computeGamma for new chunk" << endl;
      #endif
      computeGamma(memChunkStorage, GammaStorage, d, nChunkSize);
      ++(*inputArrayIter);
    }
    combineGamma(query);
    shared_ptr<Array> outputArray = writeGamma(query);
    destroy();
    return outputArray;
  }

  void copyChunkToMemory(shared_ptr<ConstChunkIterator> chunkIter) {
    Coordinates chunkPos = chunkIter->getPosition();
    Coordinates currPos;
    while ( ! chunkIter->end()) {
      currPos = chunkIter->getPosition();
      memChunk[currPos[1]-chunkPos[1]+1][currPos[0]-chunkPos[0]] = chunkIter->getItem().getDouble();
      ++(*chunkIter);
    }
  }

  void combineGamma(shared_ptr<Query> query) {
    if(query->getInstancesCount() > 1) {
      if(query->getInstanceID() != 0) {
        // I am not the coordinator, I should send my Gamma matrix out.
        shared_ptr<SharedBuffer> buf(new MemoryBuffer(GammaStorage, sizeof(double)*GammaLen, false));
        BufSend(0, buf, query);
      }
      else {
        // I am the coordinator, I should collect Gamma matrix from workers.
        for(InstanceID l = 1; l<query->getInstancesCount(); ++l) {
          shared_ptr<SharedBuffer> buf = BufReceive(l, query);
          double *Gammabuf = static_cast<double*> (buf->getData());
          for(size_t i=0; i<GammaLen; ++i) {
            GammaStorage[i] += Gammabuf[i];
          }
        }
      } // end if getInstanceID() != 0
    } //end if InstancesCount() > 1
  }

  shared_ptr<Array> writeGamma(shared_ptr<Query> query) {
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    if (query->getInstanceID() != 0) {
      return outputArray;
    }
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(2, 1);
    // The output array has only one chunk.
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
    size_t i, j;
    Value valGamma;
    for(i=0; i<d; i++) {
      position[0] = i+1;
      for(j=0; j<d; j++) {
        if(i>=j) {
          valGamma.setDouble(Gamma[i][j]);
        }
        else {
          valGamma.setDouble(Gamma[j][i]);
        }
        position[1] = j+1;
        outputChunkIter->setPosition(position);
        outputChunkIter->writeItem(valGamma);
      }
    }
    outputChunkIter->flush();
    return outputArray;
  }

  void initOperator(shared_ptr<Array> inputArray, shared_ptr<Query> query) {
    ArrayDesc inputSchema = inputArray->getArrayDesc();
    DimensionDesc dimsN = inputSchema.getDimensions()[0];
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    d = dimsD.getCurrEnd() - dimsD.getCurrStart() + 2;  // Output is (d+1)x(d+1)
    nChunkSize = dimsN.getChunkInterval();
    // GammaLen = (d+1)*d/2;
    GammaLen = d*d;
    #ifdef DEBUG
    // Create or open the log file.
    stringstream ss;
    ss << getenv("HOME") << "/GPUDenseGamma-instance-" << query->getInstanceID() << ".log";
    log.open(ss.str().c_str(), ios::out);
    log << "d = " << d << " nChunkSize = " << nChunkSize << " GammaLen = " << GammaLen << endl;
    #endif

    memChunkStorage = new double[d*nChunkSize];
    GammaStorage = new double[GammaLen];
    memChunk = new double*[d];
    Gamma = new double*[d];
    memset(memChunkStorage, 0, sizeof(double)*d*nChunkSize);
    memset(GammaStorage, 0, sizeof(double)*GammaLen);
    for (size_t i=0; i<d; i++) {
      memChunk[i] = &memChunkStorage[i*nChunkSize];
      // Gamma[i] = &GammaStorage[(i+1)*i/2];
      Gamma[i] = &GammaStorage[i*d];
    }
    for (size_t i=0; i<nChunkSize; i++) {
      memChunk[0][i] = 1.0;
    }
  }

  void destroy() {
    delete[] memChunkStorage;
    delete[] GammaStorage;
    delete[] memChunk;
    delete[] Gamma;
    #ifdef DEBUG
    log.close();
    #endif
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGPUDenseGamma, "GPUDenseGamma", "PhysicalGPUDenseGamma");

} //namespace scidb
