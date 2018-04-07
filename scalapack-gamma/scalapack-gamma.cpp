#include <mpi.h>
 
#include <iostream>
#include <iomanip>   //setw
#include <string>
#include <sstream>
#include <cstdlib>   //rand
#include <fstream>

#define DEBUG 0
 
using namespace std;
 
extern "C" {
  /* Cblacs declarations */
  void Cblacs_pinfo(int*, int*);
  void Cblacs_get(int, int, int*);
  void Cblacs_gridinit(int*, const char*, int, int);
  void Cblacs_pcoord(int, int, int*, int*);
  void Cblacs_gridexit(int);
  void Cblacs_barrier(int, const char*);
  void Cdgerv2d(int, int, int, double*, int, int, int);
  void Cdgesd2d(int, int, int, double*, int, int, int);

  /* ScaLAPACK declarations */
  int  numroc_(int*, int*, int*, int*, int*);
  void descinit_( int *desc, int *m, int *n, int *mb, int *nb, int *irsrc, 
		  int *icsrc, int *ictxt, int *lld, int *info);
  void pdgemm_(char *transa, char *transb, int *M, int *N, int *K, double *alpha,
	       double *A, int *ia, int *ja, int *desca,
	       double *B, int *ib, int *jb, int *descb, double *beta, 
	       double *C, int *ic, int *jc, int *descc);
}
 
int main(int argc, char **argv)
{
  double starttime, mytime, avgtime;
  int mpirank, mpinprocs;
  double *buf;

  MPI_File fh;
  MPI_Status status;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpirank);
  MPI_Comm_size(MPI_COMM_WORLD, &mpinprocs);
  bool mpiroot = (mpirank == 0);
 
  /* Helping vars */
  int iZERO = 0;
 
  if (argc < 6) {
    if (mpiroot){
      cerr << "Usage: scalapack-gamma-cpp n d nb db binary-input-file-name" << endl;
    }
    MPI_Finalize();
    return 1;
  }
 
  int nn, dd, nnb, ddb;
  double *XT_global = NULL, *XT_local = NULL;
  double *Gamma_global = NULL, *Gamma_local = NULL;
 
  /* Read command line arguments */
  stringstream stream;
  stream << argv[1] << " " << argv[2] << " " << argv[3] << " " << argv[4];
  stream >> nn >> dd >> nnb >> ddb;
  
#if DEBUG 
  if (mpiroot){
    cout << "n= " << nn << ", d= " << dd 
	 << ", nb= " << nnb << ", db= " << ddb << endl;
  }
#endif

  int rows = dd, columns = nn, row_blocks = ddb, column_blocks = nnb;

  /* read the input file's rank's chunk in each process) */
  MPI_File_open( MPI_COMM_WORLD, argv[5], MPI_MODE_RDONLY, MPI_INFO_NULL, &fh );
  int chunk = (rows*columns)/mpinprocs;
  buf = (double *)malloc( chunk * sizeof(double) );

  MPI_File_seek( fh, chunk*mpirank*sizeof(MPI_DOUBLE), MPI_SEEK_SET ); 

  starttime = MPI_Wtime();
  MPI_File_read_all( fh, buf, chunk, MPI_DOUBLE, &status );
  mytime = MPI_Wtime() - starttime;

  MPI_Reduce(&mytime, &avgtime, 1, MPI_DOUBLE, MPI_SUM, 0 ,MPI_COMM_WORLD);

  avgtime /= mpinprocs;
  if (mpiroot) {
    cout << endl << "Average IO (read) Time [seconds]: " << avgtime << endl << endl;
  }


  /* Reserve space for matrix XT_global */
  if (mpiroot) {
    try {
      XT_global  = new double[rows*columns];
    } catch (std::bad_alloc& ba) {
      std::cerr << "Failed to allocate memory for XT_global." << endl 
		<< "Exeprtion: " << ba.what() << endl;
      return 1;
    } 
  }

  /* Gather all the chunks in root */
  MPI_Gather( buf, chunk, MPI_DOUBLE, XT_global, chunk, MPI_DOUBLE,
	      0, MPI_COMM_WORLD);

  free( buf );
  MPI_File_close( &fh );


  if (mpiroot) {
    /* Reserve space and fill in matrix Gamma */
    try{
      Gamma_global = new double[dd*dd];
    } catch (std::bad_alloc& ba) {
      std::cerr << "Failed to allocate memory for Gamma_global." << endl 
		<< "Exeprtion: " << ba.what() << endl;
      return 1;
    } 

    /* Fill Gamma with zeros */
    for (int r = 0; r < dd; ++r) {
      for (int c = 0; c < dd; ++c) {
	*(Gamma_global+dd*c + r) = (double) 0;
      }
    }

    /* Print matrix XT (top left corner [10x10]) */
#if DEBUG
    cout << "Matrix XT (top left corner [10x10]):\n";
    for (int r = 0; r < min(rows,10); ++r) {
      for (int c = 0; c < min(columns,10); ++c) {
	cout << setw(15) << XT_global [rows*c + r] << " ";
      }
      cout << "\n";
    }
    cout << endl;
#endif
  }
 
  /* Begin Cblas context */
  /* We assume that we have 4 processes and place them in a 2-by-2 grid */
  int ctxt, myid, myrow, mycol, numproc;
  int procrows = 2, proccols = 2;
  Cblacs_pinfo(&myid, &numproc);
  Cblacs_get(0, 0, &ctxt);
  Cblacs_gridinit(&ctxt, "Row-major", procrows, proccols);
  Cblacs_pcoord(ctxt, myid, &myrow, &mycol);
 
#if DEBUG
  /* Print grid pattern */
  if (myid == 0)
    cout << "Processes grid pattern:" << endl;
  for (int r = 0; r < procrows; ++r) {
    for (int c = 0; c < proccols; ++c) {
      Cblacs_barrier(ctxt, "All");
      if (myrow == r && mycol == c) {
	cout << myid << " " << flush;
      }
    }
    Cblacs_barrier(ctxt, "All");
    if (myid == 0)
      cout << endl;
  }
#endif
 
  /*****************************************
   * HERE BEGINS THE MOST INTERESTING PART *
   *****************************************/
 
  /* Reserve space for local matrices */
  // Number of rows and cols owned by the current process
  int XT_nrows = numroc_(&rows, &row_blocks, &myrow, &iZERO, &procrows);
  int XT_ncols = numroc_(&columns, &column_blocks, &mycol, &iZERO, &proccols);
  for (int id = 0; id < numproc; ++id) {
    Cblacs_barrier(ctxt, "All");
  }
  XT_local = new double[XT_nrows*XT_ncols];
  for (int i = 0; i < XT_nrows*XT_ncols; ++i) *(XT_local+i)=0.;

  /* Scatter matrix */
  int sendr = 0, sendc = 0, recvr = 0, recvc = 0;
  for (int r = 0; r < rows; r += row_blocks, sendr=(sendr+1)%procrows) {
    sendc = 0;
    // Number of rows to be sent
    // Is this the last row block?
    int nr = row_blocks;
    if (rows-r < row_blocks)
      nr = rows-r;
 
    for (int c = 0; c < columns; c += column_blocks, sendc=(sendc+1)%proccols) {
      // Number of cols to be sent
      // Is this the last col block?
      int nc = column_blocks;
      if (columns-c < column_blocks)
	nc = columns-c;
 
      if (mpiroot) {
	// Send a nr-by-nc submatrix to process (sendr, sendc)
	Cdgesd2d(ctxt, nr, nc, XT_global+rows*c+r, rows, sendr, sendc);
      }
 
      if (myrow == sendr && mycol == sendc) {
	// Receive the same data
	// The leading dimension of the local matrix is XT_nrows!
	Cdgerv2d(ctxt, nr, nc, XT_local+XT_nrows*recvc+recvr, XT_nrows, 0, 0);
	recvc = (recvc+nc)%XT_ncols;
      }
     }
 
    if (myrow == sendr)
      recvr = (recvr+nr)%XT_nrows;
  }

  /* Reserve space for local matrices */
  // Number of rows and cols owned by the current process
  int Gamma_nrows = numroc_(&dd, &ddb, &myrow, &iZERO, &procrows);
  int Gamma_ncols = numroc_(&dd, &ddb, &mycol, &iZERO, &proccols);
  for (int id = 0; id < numproc; ++id) {
    Cblacs_barrier(ctxt, "All");
  }
  Gamma_local = new double[Gamma_nrows*Gamma_ncols];
  for (int i = 0; i < Gamma_nrows*Gamma_ncols; ++i) *(Gamma_local+i)=0.;

  /* Scatter matrix */
  sendr = 0; sendc = 0; recvr = 0; recvc = 0;
  for (int r = 0; r < dd; r += ddb, sendr=(sendr+1)%procrows) {
    sendc = 0;
    // Number of rows to be sent
    // Is this the last row block?
    int nr = ddb;
    if (dd-r < ddb)
      nr = dd-r;
 
    for (int c = 0; c < dd; c += ddb, sendc=(sendc+1)%proccols) {
      // Number of cols to be sent
      // Is this the last col block?
      int nc = ddb;
      if (dd-c < ddb)
	nc = dd-c;
 
      if (mpiroot) {
	// Send a nr-by-nc submatrix to process (sendr, sendc)
	Cdgesd2d(ctxt, nr, nc, Gamma_global+dd*c+r, dd, sendr, sendc);
      }
 
      if (myrow == sendr && mycol == sendc) {
	// Receive the same data
	// The leading dimension of the local matrix is Gamma_nrows!
	Cdgerv2d(ctxt, nr, nc, Gamma_local+Gamma_nrows*recvc+recvr,
		 Gamma_nrows, 0, 0);
	recvc = (recvc+nc)%Gamma_ncols;
      }
     }
 
    if (myrow == sendr)
      recvr = (recvr+nr)%XT_nrows;
  }

  /* Print local matrices for X (top left corner [10x10])*/
#if DEBUG
  for (int id = 0; id < numproc; ++id) {
    if (id == myid) {
      cout << "XT_local (top left corner [10x10]) on node " << myid << endl;
      for (int r = 0; r < min(XT_nrows,10); ++r) {
	for (int c = 0; c < min(XT_ncols,10); ++c)
	  cout << setw(15) << *(XT_local+XT_nrows*c+r) << " ";
	cout << endl;
      }
      cout << endl;
    }
    Cblacs_barrier(ctxt, "All");
  }
#endif

  /* Initialize matrix descriptions and multiply matrices */
  int info;
  int iONE = 1;
  double alpha = 1.0; 
  double beta = 0.0;
  int descX[9], descGamma[9];
  int lldX = max(1,XT_nrows);
  int lldGamma = max(1,Gamma_nrows);
  descinit_(descX, &rows, &columns, &row_blocks, &column_blocks, &iZERO, &iZERO, &ctxt, &lldX, &info);
  descinit_(descGamma, &dd, &dd, &ddb, &ddb, &iZERO, &iZERO, &ctxt, &lldGamma, &info);

  char N[1] = {'N'};
  char T[1] = {'T'};
  
  starttime = MPI_Wtime();
  
  /* Gamma = XT*X */ 
  pdgemm_(N, T, &dd, &dd, &nn, &alpha, XT_local, &iONE, &iONE, descX,
	  XT_local, &iONE, &iONE, descX,
	  &beta, Gamma_local, &iONE, &iONE, descGamma);

  mytime = MPI_Wtime() - starttime;
  MPI_Reduce(&mytime, &avgtime, 1, MPI_DOUBLE, MPI_SUM, 0 ,MPI_COMM_WORLD);
  avgtime /= mpinprocs;

  /* Print local matrices for Gamma (top left corner [10x10])*/
#if DEBUG
  for (int id = 0; id < numproc; ++id) {
    if (id == myid) {
      cout << "Gamma_local (top left corner [10x10]) on node " << myid << endl;
      for (int r = 0; r < min(Gamma_nrows,10); ++r) {
	for (int c = 0; c < min(Gamma_ncols,10); ++c)
	  cout << setw(15) << *(Gamma_local+Gamma_nrows*c+r) << " ";
	cout << endl;
      }
      cout << endl;
    }
    Cblacs_barrier(ctxt, "All");
  }
#endif


  /* Gather matrix Gamma*/
  sendr = 0; recvr = 0; recvc = 0;
  for (int r = 0; r < dd; r += ddb, sendr=(sendr+1)%procrows) {
    sendc = 0;
    // Number of rows to be sent
    // Is this the last row block?
    int nr = ddb;
    if (dd-r < ddb)
      nr = dd-r;
 
    for (int c = 0; c < dd; c += ddb, sendc=(sendc+1)%proccols) {
      // Number of cols to be sent
      // Is this the last col block?
      int nc = ddb;
      if (dd-c < ddb)
	nc = dd-c;
 
      if (myrow == sendr && mycol == sendc) {
	// Send a nr-by-nc submatrix to process (sendr, sendc)
	Cdgesd2d(ctxt, nr, nc, Gamma_local+Gamma_nrows*recvc+recvr, 
		 Gamma_nrows, 0, 0);
	recvc = (recvc+nc)%Gamma_ncols;
      }
 
      if (mpiroot) {
	// Receive the same data
	// The leading dimension of the local matrix is Gamma_nrows!
	Cdgerv2d(ctxt, nr, nc, Gamma_global+dd*c+r, dd, sendr, sendc);
      }
 
    }
 
    if (myrow == sendr)
      recvr = (recvr+nr)%Gamma_nrows;
  }
 
#if DEBUG
  /* Print gathered matrix Gamma (top left corner [10x10])*/
  if (mpiroot) {
    cout << "Matrix Gamma = XT*X (top left corner [10x10]):\n";
    for (int r = 0; r < min(dd,10); ++r) {
      for (int c = 0; c < min(dd,10); ++c) {
	cout << setw(15) << *(Gamma_global+dd*c+r) << " ";
      }
      cout << endl;
    }
  }
#endif

  if (mpiroot) {
    cout << endl << "Average Time [seconds]: " << avgtime << endl << endl;
  }

  /************************************
   * END OF THE MOST INTERESTING PART *
   ************************************/
 
  /* Release resources */
  delete[] XT_global;
  delete[] XT_local;

  delete[] Gamma_global;
  delete[] Gamma_local;

  Cblacs_gridexit(ctxt);
  MPI_Finalize();
  return 0;
}
