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
 * @file PhysicalCorrelation.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the Correlation SciDB operator.
 */

#include <query/Operator.h>
#include <math.h>
#include <sstream>
#include <query/Network.h>
using namespace std;

namespace scidb {

class PhysicalCorrelation : public PhysicalOperator {
public:
  
  PhysicalCorrelation(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }


  shared_ptr< Array > execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query) {
    // I maintain the log of the operator in a local file named after Correlation_N.log, N is the instance ID.
    stringstream logFileName;
    logFileName << "/home/scidb/correlation_" << query->getInstanceID() << ".log";
    FILE *logFile;
    logFile = fopen(logFileName.str().c_str(), "w");
    
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    
    shared_ptr<Array> inputArray = inputArrays[0];
    ArrayDesc inputSchema = inputArray->getArrayDesc();
    
    // Get descriptor of two dimensions d and n.
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsP = inputSchema.getDimensions()[1];
    Coordinate n = dimsN.getCurrEnd() - dimsN.getCurrStart() + 1;
    // Note: the input data set should have d+1 dimensions (including Y)
    Coordinate p = dimsP.getCurrEnd() - dimsP.getCurrStart();
    Coordinate nStart = dimsN.getCurrStart();
    Coordinate pStart = dimsP.getCurrStart(); 
    
    // Get chunk size of p and n.
    Coordinate nChunkSize = dimsN.getChunkInterval();
    Coordinate pChunkSize = dimsP.getChunkInterval();
    
    fprintf(logFile, "n = %lu, d = %lu, nChunkSize = %lu, pChunkSize = %lu\n", n, p, nChunkSize, pChunkSize);
    fprintf(logFile, "instanceId = %lu\n", query->getInstanceID());
    fflush(logFile);

    double *currRow = new double[p+1];
    double yy = 0.0;
    double ly = 0.0; 
    double *yq = new double[p];
    double *S = new double[p];
    double *L = new double[p];
    for (Coordinate i=0; i<p; i++) {
      yq[i] = 0.0;
      S[i] = 0.0;
      L[i] = 0.0;
    }
    shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getIterator(0);
    Coordinates chunkPosition;
    while(! inputArrayIter->end() ) {
      shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
      chunkPosition = inputArrayIter->getPosition();
      fprintf(logFile, "Begin to work on chunk (%lu, %lu).\n", chunkPosition[0], chunkPosition[1]);
      fflush(logFile);
      for(Coordinate i=chunkPosition[0]; i<chunkPosition[0] + nChunkSize; i++) {
        if(i == n + nStart) {
          break;
        }
        for(Coordinate j=chunkPosition[1], m=0; j<chunkPosition[1] + p + 1; j++, m++) {
          if(j == p + 1 + pStart) {
            break;
          }
          currRow[m] = chunkIter->getItem().getDouble();
          ++(*chunkIter);
        }
        for(Coordinate k=0; k<p; ++k) {
          if(currRow[k] == 0.0) {
            continue;
          }
          yq[k] += currRow[p] * currRow[k];
          S[k] += currRow[k] * currRow[k];
          L[k] += currRow[k];
        }
        ly += currRow[p];
        yy += currRow[p] * currRow[p];
      }
      ++(*inputArrayIter);
    }
    /**
     * The "logical" instance ID of the instance responsible for coordination of query.
     * COORDINATOR_INSTANCE if instance execute this query itself.
     */
    if(query->getInstancesCount() > 1) {
      // I am not the coordinator, I should send my Q matrix out.
      if(query->getInstanceID() != 0) {
        shared_ptr <SharedBuffer> buf ( new MemoryBuffer(NULL, sizeof(double) * (3*p+2) ) );
        double *corrbuf = static_cast<double*> (buf->getData());
        *corrbuf = yy; corrbuf++;
        *corrbuf = ly; corrbuf++;
        for(Coordinate i=0; i<p; i++, corrbuf++) {
          *corrbuf = yq[i];
        }
        for(Coordinate i=0; i<p; i++, corrbuf++) {
          *corrbuf = S[i];
        }
        for(Coordinate i=0; i<p; i++, corrbuf++) {
          *corrbuf = L[i];
        }
        BufSend(0, buf, query);
        fprintf(logFile, "I am not the coordinator, I sent my corr buffer out.\n");
        fflush(logFile);
        delete[] currRow;
        delete[] yq;
        delete[] S;
        delete[] L;
        return outputArray;
      }
      else {
        for(InstanceID i = 1; i<query->getInstancesCount(); i++) {
          shared_ptr<SharedBuffer> buf = BufReceive(i, query);
          double *corrbuf = static_cast<double*> (buf->getData());
          fprintf(logFile, "I am the coordinator, I received corr buffer from instance %lu.\n", i);
          fflush(logFile);
          yy += *corrbuf; corrbuf++;
          ly += *corrbuf; corrbuf++;
          for(Coordinate i=0; i<p; i++, corrbuf++) {
            yq[i] += *corrbuf;
          }
          for(Coordinate i=0; i<p; i++, corrbuf++) {
            S[i] += *corrbuf;
          }
          for(Coordinate i=0; i<p; i++, corrbuf++) {
            L[i] += *corrbuf;
          }
          fprintf(logFile, "Merged.\n");
        }
        fprintf(logFile, "ly=%lf, yy=%lf\n", ly, yy);
        for(Coordinate k=0; k<p; ++k) {
          fprintf(logFile, "yq[%ld]=%f, S[%ld]=%f, L[%ld]=%f\n", k, yq[k], k, S[k], k, L[k]);
        }
        fflush(logFile);
      }// end if getInstanceID() != 0
    }//end if InstancesCount() > 1
    double *corr = new double[p];
    double resSqrt = sqrt(n * yy - ly * ly);
    for(Coordinate i=0; i<p; i++) {
      corr[i] = (n * yq[i] - ly * L[i]) / (resSqrt * sqrt(n * S[i] - L[i] * L[i]));
    }
    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(1, 1);
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
    fprintf(logFile, "Writing corr matrix to the output array.\n");
    fflush(logFile);
    for(Coordinate i=1; i<=p; i++) {
      position[0] = i;
      outputChunkIter->setPosition(position);
      Value valCorrelation;
      valCorrelation.setDouble(corr[i-1]);
      outputChunkIter->writeItem(valCorrelation);
    }
    outputChunkIter->flush();
    fprintf(logFile,"Write complete.\n");
    fflush(logFile);
    fclose(logFile); 
    delete[] currRow;
    delete[] yq;
    delete[] S;
    delete[] L;
    delete[] corr;
    return outputArray;

  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalCorrelation, "correlation", "PhysicalCorrelation");

} //namespace scidb
