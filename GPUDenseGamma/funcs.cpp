#include <cmath>
using namespace std;
void computeGamma(double **memChunk, double **Gamma, size_t d, size_t nChunkSize) {
  for (size_t i=0; i<nChunkSize; i++) {
    for (size_t j=0; j<d; j++) {
      for (size_t k=0; k<=j; k++) {
        Gamma[j][k] += memChunk[i][j] * memChunk[i][k];
      }
    }
  }
}
