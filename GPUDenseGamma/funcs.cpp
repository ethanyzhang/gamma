#include <cstdint>
using namespace std;
void computeGamma(double *memChunk, double *Gamma, size_t d, size_t nChunkSize) {
  #pragma acc enter data copyin(memChunk[0:d*nChunkSize], Gamma[0:d*d], d, nChunkSize)
  #pragma acc kernels loop present(memChunk[0:d*nChunkSize], Gamma[0:d*d], d, nChunkSize) independent collapse(2)
  for (size_t j=0; j<d; j++) {
    for (size_t k=0; k<d; k++) {
      for (size_t i=0; i<nChunkSize; i++) {
        Gamma[j*d+k] += memChunk[j*nChunkSize+i] * memChunk[k*nChunkSize+i];
      }
    }
  }
  #pragma acc exit data copyout(Gamma[0:d*d]) delete(memChunk[0:d*nChunkSize])
}
