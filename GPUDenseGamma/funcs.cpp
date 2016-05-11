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
