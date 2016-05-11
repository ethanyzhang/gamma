#include <cstdint>
using namespace std;
void enterData(double *restrict memChunk, double *restrict Gamma, size_t d, size_t nChunkSize) {
  #pragma acc enter data copyin(Gamma[0:d*d]) create(memChunk[0:d*nChunkSize])
}

void exitData(double *restrict memChunk, double *restrict Gamma, size_t d, size_t nChunkSize) {
  #pragma acc exit data copyout(Gamma[0:d*d]) delete(memChunk[0:d*nChunkSize])
}

void computeGamma(double *restrict memChunk, double *restrict Gamma, size_t d, size_t nChunkSize) {
  #pragma acc update device(memChunk[0:d*nChunkSize])
  #pragma acc kernels loop present(memChunk[0:d*nChunkSize], Gamma[0:d*d]) \
                           copyin(d, nChunkSize) independent gang worker collapse(2)
  for (size_t j=0; j<d; j++) {
    for (size_t k=0; k<d; k++) {
      double GammaDelta = 0.0;
      #pragma acc loop reduction(+:GammaDelta) vector
      for (size_t i=0; i<nChunkSize; i++) {
        GammaDelta += memChunk[j*nChunkSize+i] * memChunk[k*nChunkSize+i];
      }
      Gamma[j*d+k] += GammaDelta;
    }
  }
}
