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

#include <cstdint>
#include <cmath>
#include <limits>     // numeric_limits<double>::max()
using namespace std;
#define UNINITIALIZED -1

double assignCluster(double **memChunk, double **centroids, double **gamma,
                     int32_t k, size_t d, size_t nChunkSize) {
  double difference, distance, closestDistance, sumDistance = 0.0;
  int32_t closestCluster;
  for (size_t row=0; row<nChunkSize; row++) {
    double *x_i = memChunk[row];
    closestDistance = numeric_limits<double>::max();
    closestCluster = UNINITIALIZED;
    for (int32_t i=0; i<k; i++) {     // for each cluster
      distance = 0;
      for (size_t j=0; j<d; j++) {
        difference = x_i[j] - centroids[i][j];
        distance += difference * difference;
      }
      if (distance < closestDistance) {
        closestDistance = distance;
        closestCluster = i;
      }
    }
    // Accumulate diagonal gamma
    gamma[closestCluster][0]++; // n
    for (size_t i=0; i<d; i++) {
      gamma[closestCluster][i+1] += x_i[i]; // L
      gamma[closestCluster][i+d+1] += x_i[i] * x_i[i];  // Q
    }
    sumDistance += sqrt(closestDistance);
  }
  return sumDistance;
}
