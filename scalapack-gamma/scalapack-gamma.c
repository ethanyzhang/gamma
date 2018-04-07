#include <stdio.h>
#include <stdlib.h>
#include "cblas.h"
#include "cblas_f77.h"
#include "mpi.h"
#include <time.h>

static int min( int a, int b ){
  if (a<b) {
    return(a); 
  } else {
    return(b);
  }
}

extern void   Cblacs_pinfo( int* mypnum, int* nprocs);
extern void   Cblacs_get( int context, int request, int* value);
extern int    Cblacs_gridinit( int* context, char * order, int np_row, int np_col);
extern void   Cblacs_gridinfo( int context, int*  np_row, int* np_col, 
			       int*  my_row, int*  my_col);
extern void   Cblacs_gridexit( int context);
extern void   Cblacs_exit( int error_code);

extern void   descinit_( int *desc, int *m, int *n, int *mb, int *nb, int *irsrc, 
			 int *icsrc, int *ictxt, int *lld, int *info);

extern void   pdgemm_(char *transa, char *transb, int *M, int *N, int *K, double *alpha,
		      double *A, int *ia, int *ja, int *desca,
		      double *B, int *ib, int *jb, int *descb, double *beta, 
		      double *C, int *ic, int *jc, int *descc);


long int timespecDiff(struct timespec *timeA_p, struct timespec *timeB_p)
{
  return ((timeA_p->tv_sec * 1000000000) + timeA_p->tv_nsec) -
    ((timeB_p->tv_sec * 1000000000) + timeB_p->tv_nsec);
}


int main( int argc, char *argv[])
{
  int iam, nprocs;
  int myrank_mpi, nprocs_mpi;
  int ictxt, nprow, npcol, myrow, mycol;
  int info;
  int descA[9], descB[9], descC[9];
  int one = 1;
  int zero = 0;

  //  X= n X d          XT= d X n
  //  X * XT = Gamma
  //  Gamma = d X d
  double *X,*XT,*Gamma;
  int d, n;
  int i, j;
  double alpha, beta;
  struct timespec  tstart, tend;
  double seconds;

  if (argc != 3) {
    printf("usage: ./exec n d\n");
    return 1;
  }

  sscanf (argv[1],"%d",&n);
  sscanf (argv[2],"%d",&d);

  printf (" Initializing data for matrix multiplication Gamma=XT*X for matrix \n"
	  " X(%ix%i) and matrix XT(%ix%i)\n\n", n, d, d, n);

  printf (" Allocating memory for matrices  \n\n");
  
  XT    = (double *) malloc( d*n*sizeof( double ));//, 64 );
  X     = (double *) malloc( n*d*sizeof( double ));//, 64 );
  Gamma = (double *) malloc( d*d*sizeof( double ));//, 64 );
  if (X == NULL || XT == NULL || Gamma == NULL) {
    printf( "\n ERROR: Can't allocate memory for matrices. Aborting... \n\n");
    free(X);
    free(XT);
    free(Gamma);
    return 1;
  }

  printf (" Intializing matrix data \n\n");
  

  //Fills X with random numbers

  for (i = 0; i < (n*d); i++) {
    X[i] = (double) rand()/ RAND_MAX;
  }


  // Fills Gamma with 0s

  for (i = 0; i < (d*d); i++) {
    Gamma[i] = 0.0;
  }

  // DGEMM: perform one of the matrix-matrix operations
  //        C = alpha*op( A )*op( B ) + beta*C,

  // The first parameter is a flag which specifies  either Row Major
  // or Column Major order

  alpha = 1.0; beta = 0.0;


  //  Starting time measurement

  clock_gettime(CLOCK_MONOTONIC,&tstart);

  // Transposes matrix X

  for (i = 0; i< n; i++) {
    for (j =0; j<d; j++) {
      XT[j*n + i ] = X[i*d + j];
    }
  }

  printf (" Computing matrix product...\n\n");
  
  // A full description of the parameters is available in CBLAS library documentation
  
  /*
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
    d, d, n, alpha, XT, n, X, d, beta, Gamma, d);
  */
  

  MPI_Init( &argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank_mpi);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs_mpi);

  printf(" My rank is: %d\n\n", myrank_mpi);


  nprow = n;
  npcol = d;

  Cblacs_pinfo( &iam, &nprocs ) ;
  Cblacs_get( -1, 0, &ictxt );
  Cblacs_gridinit( &ictxt, "R", nprow, npcol );
  Cblacs_gridinfo( ictxt, &nprow, &npcol, &myrow, &mycol );


  //test_begin  //for testing purposes
  /* Print grid pattern */
  int r,c;
  if (iam == 0)
    printf("Processes grid pattern:\n");
  for (r = 0; r < nprow; ++r) {
    for (c = 0; c < npcol; ++c) {
      Cblacs_barrier(ictxt, "All");
      if (myrow == r && mycol == c) {
	printf("%d ", iam );
	fflush(stdout);
      }
    }
    Cblacs_barrier(ictxt, "All");
    if (iam == 0)
      printf("\n");
  }
  //test_end


  descinit_(descA, &n, &d, &n, &d, &zero, &zero, &ictxt, &n, &info);
  descinit_(descB, &d, &n, &d, &n, &zero, &zero, &ictxt, &d, &info);
  descinit_(descC, &d, &d, &d, &d, &zero, &zero, &ictxt, &d, &info);

  printf("DEBUGGING!\n");


  pdgemm_("N", "N", &d, &d, &n, &alpha, XT, &one, &one, descA, X, &one, &one, descB, &beta, Gamma, &one, &one, descC);

  // Stopping time measurement

  clock_gettime(CLOCK_MONOTONIC,&tend);
  long int  timeElapsed =timespecDiff(&tend, &tstart);
  if (myrank_mpi == 0){
    printf (" computations completed.\n\n");
  }
  printf("Time:%li", timeElapsed);

  if (myrank_mpi == 0){
    printf ("\n Top left corner of matrix X: \n");
 
    for (i=0; i<min(n,6); i++) {
      for (j=0; j<min(d,6); j++) {
	printf ("%12.5G", X[i*d+j]);
      }
      printf ("\n");
    }
    

    printf (" Top left corner of matrix XT: \n");
    for (i=0; i<min(d,6); i++) {
      for (j=0; j<min(n,6); j++) {
	printf ("%12.5G", XT[i*n+j]);
      }
      printf ("\n");
    }

    printf ("\n Top left corner of matrix Gamma: \n");
    for (i=0; i<min(d,6); i++) {
      for (j=0; j<min(d,6); j++) {
	printf ("%12.5G", Gamma[i*d +j]);
      }
      printf ("\n");
    }
    
    printf ("\n Deallocating memory \n\n");
  }
  free(X);
  free(XT);
  free(Gamma);
  if (myrank_mpi == 0){
    printf (" Example completed.\n\n");
  }
  Cblacs_gridexit( ictxt );
  MPI_Finalize();
  return 0;
}
