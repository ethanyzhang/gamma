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
using namespace std;

#ifdef NOGPU
void enterDataRegion(double *memChunk, double *Gamma, size_t d, size_t nChunkSize) { }
#else
void enterDataRegion(double *restrict memChunk, double *restrict Gamma, size_t d, size_t nChunkSize) {
  #pragma acc enter data copyin(Gamma[0:d*d]) create(memChunk[0:d*nChunkSize])
}
#endif

#ifdef NOGPU
void exitDataRegion(double *memChunk, double *Gamma, size_t d, size_t nChunkSize) { }
#else
void exitDataRegion(double *restrict memChunk, double *restrict Gamma, size_t d, size_t nChunkSize) {
  #pragma acc exit data copyout(Gamma[0:d*d]) delete(memChunk[0:d*nChunkSize])
}
#endif

#ifdef NOGPU
void computeGamma(double *memChunk, double *Gamma, size_t d, size_t nChunkSize) {
#else
void computeGamma(double *restrict memChunk, double *restrict Gamma, size_t d, size_t nChunkSize) {
  #pragma acc update device(memChunk[0:d*nChunkSize])
  #pragma acc kernels loop present(memChunk[0:d*nChunkSize], Gamma[0:d*d]) \
                           copyin(d, nChunkSize) independent
#endif
  for (size_t j=0; j<d; j++) {
    for (size_t k=0; k<=j; k++) {
      double GammaDelta = 0.0;
      #ifndef NOGPU
      #pragma acc loop reduction(+:GammaDelta) vector
      #endif
      for (size_t i=0; i<nChunkSize; i++) {
        GammaDelta += memChunk[j*nChunkSize+i] * memChunk[k*nChunkSize+i];
      }
      Gamma[j*d+k] += GammaDelta;
    }
  }
}
