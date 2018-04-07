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
 * @file PhysicalKMeans.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the KMeans SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <fstream>    // Needed for log file.
#include <sstream>    // Needed for log file name generation, config string parsing
#include <string>
#include <limits>     // numeric_limits<double>::max()
#include <cmath>      // fabs()
#include <algorithm>  // sort()
#include <map>
#define THRESHOLD 1e-4
#define UNINITIALIZED -1

using namespace std;
namespace scidb {

class PhysicalKMeans : public PhysicalOperator {
public:
  ofstream log;
  size_t n;                     // number of data points
  size_t d;                     // number of attributes
  size_t dStart;                // d dimension start offset
  int32_t k;                    // number of clusters
  int32_t imax;                 // max number of iterations
  double lastAverageDistance;
  double sumDistance;
  double omega;                 // The cluster weight threshold for reseeding.
  bool converged;
  InstanceID coordinatorInstanceId;
  // Configs
  bool incremental;
  // Lengths
  size_t centroidsLen;          // length of the centroid storage
  size_t gammaLen;              // length of the gamma storage
  // Storage
  double* centroidsStorage;
  double* variancesStorage;
  double* gammaStorage;
  // Convenience pointers
  double** centroids;
  double** variances;
  double** gamma;

  struct clusterWeight {
    int32_t clusterId;
    double weight;
    bool operator<(const clusterWeight& rhs) const { return weight < rhs.weight; }
  };
  clusterWeight* weights;

  shared_ptr<Array> execute(vector< shared_ptr< Array>>& inputArrays, shared_ptr<Query> query) {
    shared_ptr<Array> inputArray = inputArrays[0];
    initOperator(inputArray, query);
    initCentroidsWithFirstKPoints(inputArray, query);
    if (incremental) {
      incrementalKmeans(inputArray, query);
    }
    else {
      iterativeKmeans(inputArray, query);
    }
    // Write results
    shared_ptr<Array> outputArray = writeResults(query);
    destroy();
    return outputArray;
  }

  void initOperator(shared_ptr<Array> inputArray, shared_ptr<Query> query) {
    // Create or open the log file.
    stringstream ss;
    ss << getenv("HOME") << "/kmeans-instance-" << query->getInstanceID() << ".log";
    log.open(ss.str().c_str(), ios::out);

    // Get n, d, dStart, k, imax.
    ArrayDesc inputSchema = inputArray->getArrayDesc();
    DimensionDesc dimsN = inputSchema.getDimensions()[0]; 
    DimensionDesc dimsD = inputSchema.getDimensions()[1];
    n = dimsN.getCurrEnd() - dimsN.getCurrStart() + 1;
    d = dimsD.getCurrEnd() - dimsD.getCurrStart() + 1;
    dStart = dimsD.getCurrStart();
    k = ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
    imax = ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])->getExpression()->evaluate().getInt32();
    parseConfigString();

    // Allocate resources
    // Average distance (partial) is attached to the gamma sent to the coordinator.
    gammaLen = k * (2*d+1) + 1;
    // A convergence flag is attached to the centroids sent to the workers.
    centroidsLen = k * d + 1;
    weights = new clusterWeight[k];
    centroidsStorage = new double[centroidsLen];
    variancesStorage = new double[centroidsLen];
    gammaStorage = new double[gammaLen];
    centroids = new double*[k];
    variances = new double*[k];
    gamma = new double*[k];
    memset(weights, 0, sizeof(clusterWeight) * k);
    memset(centroidsStorage, 0, sizeof(double) * centroidsLen);
    memset(variancesStorage, 0, sizeof(double) * centroidsLen);
    memset(gammaStorage, 0, sizeof(double) * gammaLen);
    for (int32_t i=0; i<k; i++) {
      gamma[i] = &gammaStorage[i*(2*d+1)];
      centroids[i] = &centroidsStorage[i*d];
      variances[i] = &variancesStorage[i*d];
    }
  }

  void initCentroidsWithFirstKPoints(shared_ptr<Array> inputArray, shared_ptr<Query> query) {
    // Select the initial centroids on the coordinator.
    if (query->getInstanceID() == coordinatorInstanceId) {
      shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
      Coordinates cellPosition;
      Coordinate currRow = UNINITIALIZED;
      int32_t centroidsGot = UNINITIALIZED;
      while ( ! inputArrayIter->end() && centroidsGot < k) {
        shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
        while ( ! chunkIter->end()) {
          cellPosition = chunkIter->getPosition();
          if (currRow != cellPosition[0]) {   // Reach a new row.
            centroidsGot++;
            currRow = cellPosition[0];
            if (centroidsGot == k)
              break;
          }
          centroids[centroidsGot][cellPosition[1]-dStart] = chunkIter->getItem().getDouble();
          ++(*chunkIter);
        }
        ++(*inputArrayIter);
      }
    }
    // Broadcast the centroids to all the instances.
    syncBuffer(query, coordinatorInstanceId, centroidsStorage, sizeof(double)*centroidsLen);
    #ifdef DEBUG
    log << "Initializing centroids using the first k points..." << endl;
    printCentroids();
    #endif
  }

  void iterativeKmeans(shared_ptr<Array> inputArray, shared_ptr<Query> query) {
    double *x_i = new double[d];
    converged = false;
    // Iterations
    for (int32_t i=0; i<imax; i++) {
      #ifdef DEBUG
      log << endl << "Iterative iteration " << i+1 << " =======>" << endl;
      log << "Assigning data points to clusters..." << endl;
      #endif
      // reset the gamma vectors.
      memset(gammaStorage, 0, sizeof(double) * gammaLen);
      sumDistance = 0;

      shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
      Coordinates cellPosition;
      Coordinate currRow = UNINITIALIZED;    // -1 indicates uninitialized.
      while ( ! inputArrayIter->end()) {
        shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
        while ( ! chunkIter->end()) {
          cellPosition = chunkIter->getPosition();
          if (currRow != cellPosition[0]) { // new data record.
            if (currRow != UNINITIALIZED) {
              assignCluster(x_i);
            }
            currRow = cellPosition[0];
          }
          x_i[cellPosition[1]-dStart] = chunkIter->getItem().getDouble();
          ++(*chunkIter);
        }
        assignCluster(x_i);
        ++(*inputArrayIter);
      }
      attachAverageDistanceToGamma(sumDistance/n);
      combineGamma(query);
      converged = fabs(getAverageDistanceFromGamma() - lastAverageDistance) <= THRESHOLD;
      attachConvergenceFlagToCentroids(converged);
      lastAverageDistance = getAverageDistanceFromGamma();
      log << i+1 <<"> Average distance = " << lastAverageDistance << endl;
      recalcCentroids(query);
      if (i < (imax-1)/10+1) {
        reseed(query);
      }
      // Sync centroids, the converged flag is also synced.
      syncBuffer(query, coordinatorInstanceId, centroidsStorage, sizeof(double)*centroidsLen);
      #ifdef DEBUG
      printCentroids();
      #endif
      if (getConvergenceFlagFromCentroids()) {
        #ifdef DEBUG
        log << "Converged, break." << endl;
        #endif
        break;
      }
    }
    delete[] x_i;
  }

  void incrementalKmeans(shared_ptr<Array> inputArray, shared_ptr<Query> query) {
    double *x_i = new double[d];
    converged = false;
    // Iterations
    for (int32_t i=0; i<imax; i++) {
      #ifdef DEBUG
      log << endl << "Incremental iteration " << i+1 << " =======>" << endl;
      log << "Assigning data points to clusters..." << endl;
      #endif

      sumDistance = 0;
      shared_ptr<ConstArrayIterator> inputArrayIter = inputArray->getConstIterator(0);
      Coordinates cellPosition;
      Coordinate currRow = UNINITIALIZED;
      while ( ! inputArrayIter->end()) {
        shared_ptr<ConstChunkIterator> chunkIter = inputArrayIter->getChunk().getConstIterator();
        // reset the gamma vectors.
        memset(gammaStorage, 0, sizeof(double) * gammaLen);
        while ( ! chunkIter->end()) {
          cellPosition = chunkIter->getPosition();
          if (currRow != cellPosition[0]) { // new data record.
            if (currRow != UNINITIALIZED) {
              assignCluster(x_i);
            }
            currRow = cellPosition[0];
          }
          x_i[cellPosition[1]-dStart] = chunkIter->getItem().getDouble();
          ++(*chunkIter);
        }
        assignCluster(x_i);
        // Finish processing this chunk...
        attachAverageDistanceToGamma(sumDistance/n);
        combineGamma(query);
        converged = fabs(getAverageDistanceFromGamma() - lastAverageDistance) <= THRESHOLD;
        attachConvergenceFlagToCentroids(converged);
        recalcCentroids(query);
        if (i == 0) {
          reseed(query);
        }
        syncBuffer(query, coordinatorInstanceId, centroidsStorage, sizeof(double)*centroidsLen);
        ++(*inputArrayIter);
      }

      // syncBuffer(query, 0, &converged, sizeof(bool));
      lastAverageDistance = getAverageDistanceFromGamma();
      #ifdef DEBUG
      printCentroids();
      #endif
      log << i+1 <<"> Average distance = " << lastAverageDistance << endl;
      if (getConvergenceFlagFromCentroids()) {
        #ifdef DEBUG
        log << "Converged, break." << endl;
        #endif
        break;
      }
    }
    delete[] x_i;
  }

  void syncBuffer(shared_ptr<Query> query, InstanceID src, void *buffer, size_t size) {
    #ifdef DEBUG
    log << "syncBuffer() =======>" << endl;
    #endif
    if (query->getInstanceID() == src) {
      // Build the SharedBuffer for sending, note that the last parameter "false" means no copy.
      shared_ptr<SharedBuffer> sendBuf(new MemoryBuffer(buffer, size, false));
      for (InstanceID l = 0; l<query->getInstancesCount(); ++l) {
        if (l == src)
          continue;
        BufSend(l, sendBuf, query);
      }
    }
    else {
      shared_ptr<SharedBuffer> recvBuf = BufReceive(src, query);
      void* recvBufPtr = static_cast<void*> (recvBuf->getData());
      memcpy(buffer, recvBufPtr, size);
    }
  }

  void assignCluster(double* x_i) {
    double difference;
    double distance;
    double closestDistance = numeric_limits<double>::max();
    int32_t closestCluster = UNINITIALIZED;
    for (int32_t i=0; i<k; i++) {     // for each cluster
      distance = 0;
      for (uint32_t j=0; j<d; j++) {
        difference = x_i[j] - centroids[i][j];
        distance += difference * difference;
      }
      if (distance < closestDistance) {
        closestDistance = distance;
        closestCluster = i;
      }
    }
    sumDistance += sqrt(closestDistance);
    // Accumulate diagonal gamma
    gamma[closestCluster][0]++; // n
    for (uint32_t i=0; i<d; i++) {
      gamma[closestCluster][i+1] += x_i[i]; // L
      gamma[closestCluster][i+d+1] += x_i[i] * x_i[i];  // Q
    }
  }

  void combineGamma(shared_ptr<Query> query) {
    if (query->getInstanceID() == coordinatorInstanceId) {
      for (InstanceID l = 1; l<query->getInstancesCount(); ++l) {
        shared_ptr<SharedBuffer> recvBuf = BufReceive(l, query);
        double* recvBufPtr = static_cast<double*> (recvBuf->getData());
        for (size_t i=0; i<gammaLen; i++) {
          // The attached average distance is also combined.
          gammaStorage[i] += recvBufPtr[i];
        }
      }
    }
    else {
      shared_ptr<SharedBuffer> sendBuf(new MemoryBuffer(gammaStorage, sizeof(double)*gammaLen, false));
      BufSend(0, sendBuf, query);
    }
  }

  void reseed(shared_ptr<Query> query) {
    if (query->getInstanceID() == coordinatorInstanceId) {
      if (omega == 0) {
        return;
      }
      #ifdef DEBUG
      log << "Reseeding..." << endl;
      #endif
      sort(weights, weights+k);
      for (int32_t i=0; i<k/2; i++) {
        if (weights[i].weight < omega/k) {
          int32_t light = weights[i].clusterId;
          int32_t heavy = weights[k-1-i].clusterId;
          #ifdef DEBUG
          log << "light = " << light << " heavy = " << heavy << endl;
          printPoint(centroids[light], d);
          #endif
          for (uint32_t j=0; j<d; j++) {
            centroids[light][j] = centroids[heavy][j] + 0.5 * sqrt(variances[heavy][j]);
            variances[light][j] = 0;
          }
          #ifdef DEBUG
          log << " ==> ";
          printPoint(centroids[light], d);
          #endif
        }
      }
    }
  }

  void recalcCentroids(shared_ptr<Query> query) {
    // only the coordinator will calculate the new centroids.
    if (query->getInstanceID() == coordinatorInstanceId) {
      #ifdef DEBUG
      log << "Recalculating the centroids..." << endl;
      #endif
      double sumCount = 0;
      for (int32_t i=0; i<k; i++) {
        double count = gamma[i][0];
        sumCount += count;
        weights[i].clusterId = i;
        weights[i].weight = count;
        #ifdef DEBUG
        log << "  Cluster " << i << ": " << count << " points." << endl;
        #endif
        if (count == 0) {
          #ifdef DEBUG
          log << "  continue;" << endl;
          #endif
          continue;
        } 
        for (uint32_t j=0; j<d; j++) {
          centroids[i][j] = gamma[i][j+1] / count;
          variances[i][j] = gamma[i][j+1+d] / count - centroids[i][j] * centroids[i][j];
        }
      } // end for i
      for (int32_t i=0; i<k; i++) {
        weights[i].weight /= sumCount;
      }
    }
  }

  static bool sortWeightById(const clusterWeight &lhs, const clusterWeight &rhs) {
    return lhs.clusterId<rhs.clusterId;
  }

  shared_ptr<Array> writeResults(shared_ptr<Query> query) {
    // Output array and its iterator for all the chunks inside that array.
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    if (query->getInstanceID() != coordinatorInstanceId) {
      return outputArray;
    }
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    shared_ptr<ChunkIterator> outputChunkIter;
    sort(weights, weights+k, &PhysicalKMeans::sortWeightById);
    Coordinates position(2, 1);
    // The output array has only one chunk.
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
    Value val;
    for (int32_t i=0; i<k; i++) {
      position[0] = i+1;
      position[1] = 1;
      // Write W:
      outputChunkIter->setPosition(position);
      val.setDouble(weights[i].weight);
      outputChunkIter->writeItem(val);
      // Write C:
      for (size_t j=0; j<d; j++) {
        position[1] = j+2;
        outputChunkIter->setPosition(position);
        val.setDouble(centroids[i][j]);
        outputChunkIter->writeItem(val);
      }
      // Write R:
      for (size_t j=0; j<d; j++) {
        position[1] = j+d+2;
        outputChunkIter->setPosition(position);
        val.setDouble(variances[i][j]);
        outputChunkIter->writeItem(val);
      }
      // Write average distance:
      position[1] = d+d+2;
      outputChunkIter->setPosition(position);
      val.setDouble(lastAverageDistance);
      outputChunkIter->writeItem(val);
    }
    outputChunkIter->flush();
    return outputArray;
  }

  void attachConvergenceFlagToCentroids(bool value) {
    if (value)
      centroidsStorage[centroidsLen - 1] = 1;
    else
      centroidsStorage[centroidsLen - 1] = 0;
  }

  bool getConvergenceFlagFromCentroids() {
    return centroidsStorage[centroidsLen - 1] == 1;
  }

  void attachAverageDistanceToGamma(double avg) {
    gammaStorage[gammaLen - 1] = avg;
  }

  double getAverageDistanceFromGamma() {
    return gammaStorage[gammaLen - 1];
  }

  void destroy() {
    log << "Cleanning up..." << endl;
    log.close();
    delete[] weights;
    delete[] centroidsStorage;
    delete[] variancesStorage;
    delete[] gammaStorage;
    delete[] centroids;
    delete[] variances;
    delete[] gamma;
  }
  
  PhysicalKMeans(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema) {
    n = d = dStart = centroidsLen = gammaLen = 0;
    k = imax = 0;
    omega = sumDistance = 0;
    coordinatorInstanceId = 0;
    lastAverageDistance = numeric_limits<double>::max();
    centroidsStorage = gammaStorage = variancesStorage = NULL;
    weights = NULL;
    centroids = gamma = NULL;
    converged = incremental = false;
  }

  string getConfig(map<string, string> &argumentMap, string key) {
    map<string, string>::iterator iter = argumentMap.find(key);
    //If the argument is not found, return a blank string.
    if(iter == argumentMap.end()) {
      return "";
    }
    else {
      return iter->second;
    }
  }

  bool getBoolConfig(map<string, string> &argumentMap, string key) {
    return getConfig(argumentMap, key).compare("true") == 0;
  }

  double getDoubleConfig(map<string, string> &argumentMap, string key) {
    string value = getConfig(argumentMap, key);
    if (value.compare("") == 0)
      return 0;
    else
      return stod(value);
  }

  void parseConfigString() {
    incremental = false;
    if (_parameters.size() != 3) {
      // config string not provided.
      return;
    }
    string config = ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2])->getExpression()->evaluate().getString();
    map<string, string> argumentMap;
    #ifdef DEBUG
    log << "configString = " << config << endl;
    #endif
    stringstream currentArgumentName;
    stringstream currentArgumentValue;
    char delimiter = ',';
    bool argumentNameFinished = false;
    for (unsigned int i=0; i<=config.length(); ++i) {
      if (i == config.length() || config[i] == delimiter) {
        if(currentArgumentName.str() != "") {
            argumentMap[currentArgumentName.str()] = currentArgumentValue.str();
        }
        // reset
        currentArgumentName.str("");
        currentArgumentValue.str("");
        argumentNameFinished = false;
      }
      else if(config[i] == '=') {
        argumentNameFinished = true;
      }
      else {
        if(argumentNameFinished) {
          currentArgumentValue << config[i];
        }
        else {
          // ignore any spaces in argument names. 
          if(config[i] == ' ')
            continue;
          currentArgumentName << config[i];
        }
      }
    } // end for
    incremental = getBoolConfig(argumentMap, "incremental");
    omega = getDoubleConfig(argumentMap, "omega");
    #ifdef DEBUG
    log << "incremental = " << incremental << endl;
    log << "omega = " << omega << endl;
    #endif
  }

  #ifdef DEBUG
  void printPoint(double* dp, size_t d) {
    log << "(";
    for (size_t i=0; i<d; i++) {
      log << dp[i] << ", ";
    }
    log << "\b\b)" << endl;
  }

  void printCentroids() {
    for (int32_t i=0; i<k; i++) {
      log << "Centroid " << i+1 << ": ";
      printPoint(centroids[i], d);
    }
  }

  void printGamma() {
    for (int32_t i=0; i<k; i++) {
      log << "n = " << gamma[i][0] << endl;
      log << "L = ";
      printPoint(&gamma[i][1], d);
      log << "Q = ";
      printPoint(&gamma[i][d+1], d);
    }
  }
  #endif

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalKMeans, "KMeans", "PhysicalKMeans");

} //namespace scidb
