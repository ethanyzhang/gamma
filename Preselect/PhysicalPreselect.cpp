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
 * @file PhysicalPreselect.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the Preselect SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
// We need quick sort.
#include <stdlib.h>
#include <math.h>
#include <sstream>
using namespace std;

namespace scidb {
  
struct correlation {
  int id;
  double corr;
};

class PhysicalPreselect : public PhysicalOperator {
public:
  
    PhysicalPreselect(string const& logicalName,
                           string const& physicalName,
                           Parameters const& parameters,
                           ArrayDesc const& schema):
      PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

  // for quick sort.
  static int comp(const void* p1, const void* p2) {
    correlation* arr1 = (correlation*)p1;
    correlation* arr2 = (correlation*)p2;
    if(abs(arr1->corr) > abs(arr2->corr)) {
      return -1;
    }
    else if(arr1->corr == arr2->corr) {
      return 0;
    }
    else {
      return 1;
    }
  }

    shared_ptr< Array > execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query) {
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    // I maintain the log of the operator in a local file named after Correlation_N.log, N is the instance ID.
    stringstream logFileName;
    logFileName << "/home/scidb/preselect_" << query->getInstanceID() << ".log";
    FILE *logFile;
    logFile = fopen(logFileName.str().c_str(), "w");
    
    shared_ptr<Array> originalArray = inputArrays[0];
    shared_ptr<Array> correlationArray = inputArrays[1];
    
    ArrayDesc originalSchema = originalArray->getArrayDesc();
    ArrayDesc corrSchema = correlationArray->getArrayDesc();
    
    Dimensions originalDims = originalSchema.getDimensions();
    Dimensions corrDims = corrSchema.getDimensions();
    
    DimensionDesc originalDimsN = originalDims[0];
    DimensionDesc originalDimsP = originalDims[1];
    DimensionDesc corrDimsP = corrDims[0];
    
    Coordinate nChunkSize = originalDimsN.getChunkInterval();
    
    Coordinate nStart = originalDimsN.getCurrStart();
    Coordinate pStart = originalDimsP.getCurrStart(); 
      
    Coordinate n = originalDimsN.getCurrLength();
    // Note the correlation array doesn't have Y column.
    Coordinate p = corrDimsP.getCurrLength();
    
    //fprintf(logFile, "p = %ld\n # of chunk = %ld\n", p, corrSchema.getNumberOfChunks());
    fflush(logFile);
    
    shared_ptr<ConstArrayIterator> corrArrayIter = correlationArray->getIterator(0);
    Coordinate d = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();
    bool *selected = new bool[p+1];
    for(Coordinate i=0; i<p; ++i) {
      selected[i] = false;
    }
    selected[p] = true;
    if(query->getInstanceID() == 0) {
      correlation *corr = new correlation[p];
      // I am the coordinator
      // The correlation array will always have only 1 chunk (we designed correlation array like this), so no loops here.
      shared_ptr<ConstChunkIterator> corrChunkIter = corrArrayIter->getChunk().getConstIterator();
      for(Coordinate i=0; i<p; ++i) {
        corr[i].id = i+1;
        corr[i].corr = corrChunkIter->getItem().getDouble();
        ++(*corrChunkIter);
      }
      qsort(corr, p, sizeof(correlation), &comp);
      for(Coordinate i=0; i<d; ++i) {
        selected[corr[i].id-1] = true;
      }
      for(Coordinate i=0; i<p; ++i) {
        fprintf(logFile, "%d, %f\n", corr[i].id, corr[i].corr);
      }
      fflush(logFile);
    
      fprintf(logFile, "I am the coordinator, I will send selected column ids out.\n");

      if(query->getInstancesCount() > 1) {
        shared_ptr <SharedBuffer> buf ( new MemoryBuffer(NULL, sizeof(bool) * (p+1) ) );
        bool *flags = static_cast<bool*> (buf->getData());
        for(Coordinate i=0; i<=p; ++i, ++flags) {
          *flags = selected[i];
        }
        for(InstanceID i = 1; i<query->getInstancesCount(); ++i) {
          BufSend(i, buf, query);
        }
      }
    }
    else {
      // I am the worker.
      fprintf(logFile, "I am a worker, I will receive selected column ids from coordinator.\n");
      shared_ptr<SharedBuffer> buf = BufReceive(0, query);
      bool *flags = static_cast<bool*> (buf->getData());
      for(Coordinate i=0; i<=p; ++i, ++flags) {
        selected[i] = *flags;
        if(selected[i]) {
          fprintf(logFile, "%ld selected\n", i);
          fflush(logFile);
        }
      }
    }

    shared_ptr<ConstArrayIterator> originalArrayIter = originalArray->getIterator(0);
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    shared_ptr<ChunkIterator> outputChunkIter;
    fprintf(logFile, "Now begin to filter array.\n");

    while(! originalArrayIter->end() ) {
      shared_ptr<ConstChunkIterator> originalChunkIter = originalArrayIter->getChunk().getConstIterator();
      Coordinates chunkPosition = originalArrayIter->getPosition();
      outputChunkIter = outputArrayIter->newChunk(chunkPosition).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
      fprintf(logFile, "Begin to work on chunk (%lu, %lu).\n", chunkPosition[0], chunkPosition[1]);
      fflush(logFile);
      Coordinates outputPos(2, 1);
      for(Coordinate i=chunkPosition[0]; i<chunkPosition[0] + nChunkSize; ++i) {
        if(i == n + nStart) {
          break;
        }
        outputPos[0] = i;
        outputPos[1] = pStart;
        for(Coordinate j=chunkPosition[1]; j<chunkPosition[1] + p + 1; ++j) {
          if(j == p + 1 + pStart) {
            break;
          }
          if(selected[j-pStart]) {
            fprintf(logFile, "(%ld, %ld) selected\n", i, j);
            outputChunkIter->setPosition(outputPos);
            outputChunkIter->writeItem(originalChunkIter->getItem());
            ++outputPos[1];
          }
          ++(*originalChunkIter);
        }
        fflush(logFile);
      }
      outputChunkIter->flush();
      ++(*originalArrayIter);
    }
    delete[] selected;
    fclose(logFile);
    return outputArray;
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalPreselect, "preselect", "PhysicalPreselect");

} //namespace scidb
