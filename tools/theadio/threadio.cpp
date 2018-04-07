/*
 * Copyright (C) 2013-2018 Yiqun Zhang <contact@yzhang.io>
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

#include <iostream>
#include <fstream>    // For file stream I/O.
#include <cstdio>     // For STDIO.
#include <unistd.h>   // For POSIX functions.
#include <fcntl.h>
#include <pthread.h>
#include <thread>
#include <string>
#include <cstdint>
#include "ArgumentManager.h"
using namespace std;

// Error numbers
#define ERROR_INVALID_ARG 1
#define ERROR_OPEN_FILE 2
#define ERROR_READ 3
#define ERROR_CREATE_THREAD 4

enum ThreadImpl { t_INVALID, t_POSIX, t_CPP11 };
enum IOImpl { i_INVALID, i_POSIX, i_STDIO, i_FSTREAM };

string filename;
int64_t flen;            // file length
int8_t nthread;          // number of threads
int32_t blocksize;
ThreadImpl threadImpl;
IOImpl ioImpl;

void help() {
  cout << endl << "Usage:" << endl;
  cout << "  thread_io \"file=[file];nthread=[nThreads];thread=[c++11|posix];io=[posix|stdio|fstream];blocksize=[blocksize in bytes]\"" << endl;
  cout << endl << "Example:" << endl;
  cout << "  ./thread_io \"file=n010kd100.dat;nthread=4;thread=posix;io=posix;blocksize=1000\"" << endl << endl;
  exit(0);
}

void processArgs( int argc, char *argv[] ) {
  if ( argc == 1 ) {
    help();
  }

  ArgumentManager am(argc, argv);
  string strThreadImpl = am.get("thread");
  string strIOImpl = am.get("io");
  filename = am.get("file");
  nthread = stoi( am.get("nthread") );
  blocksize = stoi( am.get("blocksize") );
  
  if ( strThreadImpl == "posix" ) {
    threadImpl = t_POSIX;
  }
  else if ( strThreadImpl == "c++11" ) {
    threadImpl = t_CPP11;
  }
  else {
    threadImpl = t_INVALID;
  }

  if ( strIOImpl == "posix" ) {
    ioImpl = i_POSIX;
  }
  else if ( strIOImpl == "stdio" ) {
    ioImpl = i_STDIO;
  }
  else if ( strIOImpl == "fstream" ) {
    ioImpl = i_FSTREAM;
  }
  else {
    ioImpl = i_INVALID;
  }

  // check if all the arguments are valid.
  if ( filename.empty() || nthread < 1 || blocksize < 1 || threadImpl == t_INVALID || ioImpl == i_INVALID ) {
    cerr << "ERROR: Invalid arguments." << endl;
    exit(ERROR_INVALID_ARG);
  }
}

void getFileLength() {
  int32_t fd = -1;
  if ( ( fd = open( filename.c_str(), O_RDONLY ) ) < 0 ) {
    cerr << "Failed to open the file " << filename << " in function getFileLength()." << endl;
    exit(ERROR_OPEN_FILE);
  }
  flen = lseek( fd, (size_t)0, SEEK_END );
  close(fd);
}

bool m_open( int32_t *fd, FILE **fp, ifstream *ifs ) {
  if ( ioImpl == i_POSIX ) {
    *fd = open( filename.c_str(), O_RDONLY );
    return *fd >= 0;
  }
  else if ( ioImpl == i_STDIO ) {
    *fp = fopen( filename.c_str(), "rb" );
    return *fp != NULL;
  }
  else if ( ioImpl == i_FSTREAM ) {
    ifs->open( filename.c_str(), ios::in | ios::binary );
    return ifs->is_open();
  }
  return false;
}

void m_seek( size_t offset, int32_t *fd, FILE *fp, ifstream *ifs ) {
  if ( ioImpl == i_POSIX ) {
    lseek( *fd, offset, SEEK_SET );
  }
  else if ( ioImpl == i_STDIO ) {
    fseek( fp, offset, SEEK_SET );
  }
  else if ( ioImpl == i_FSTREAM ) {
    ifs->seekg( offset, ios::beg );
  }
}

ssize_t m_read( char *buf, int32_t length, int32_t *fd, FILE *fp, ifstream *ifs ) {
  if ( ioImpl == i_POSIX ) {
    return read( *fd, buf, length );
  }
  else if ( ioImpl == i_STDIO ) {
    return fread( buf, 1, length, fp );
  }
  else if ( ioImpl == i_FSTREAM ) {
    ifs->read( buf, length );
    return ifs->gcount();
  }
  return -1;
}

void m_close( int32_t *fd, FILE *fp, ifstream *ifs ) {
  if ( ioImpl == i_POSIX ) {
    close(*fd);
  }
  else if ( ioImpl == i_STDIO ) {
    fclose(fp);
  }
  else if ( ioImpl == i_FSTREAM ) {
    ifs->close();
  }
}

void *threadedRead( void *thread_id ) {
  int64_t tid = (int64_t) thread_id;
  int64_t quotient = flen / nthread;
  int64_t remainder = flen % nthread;    // last thread will do some extra work.
  int64_t offset = tid * quotient;
  int64_t workload = ( tid == nthread-1 ? quotient + remainder : quotient );
  int64_t readbytes = 0;
  int64_t nblock = 1 + ( (workload-1) / blocksize ); // ceil( workload / blocksize )
  int64_t blockseq = 0;
  char *buf = new char[ blocksize ];
  int64_t lastblocksize = workload % blocksize;

  int32_t fd = -1;
  FILE *fp = NULL;
  ifstream ifs;

  if ( lastblocksize == 0 ) lastblocksize = blocksize;

  if ( ! m_open( &fd, &fp, &ifs ) ) {
    cerr << "[Thread " << tid << "] Failed to open the file " << filename << " in function threadedRead()." << endl;
    delete[] buf;
    exit( ERROR_OPEN_FILE );
  }
  m_seek( offset, &fd, fp, &ifs );

  for (blockseq = 1; blockseq <= nblock; ++blockseq) {
    if ( blockseq == nblock ) {
      readbytes = m_read( buf, lastblocksize, &fd, fp, &ifs );
    }
    else {
      readbytes = m_read( buf, blocksize, &fd, fp, &ifs );
    }
    if ( readbytes < blocksize &&
       ( blockseq != nblock || readbytes != lastblocksize ) ) {
      cerr << "[Thread " << tid << "] Error reading block " << blockseq << ", offset " <<
          offset + (blockseq-1) * blocksize << " bytes, block size " << 
          blocksize << " bytes, only " << readbytes << " bytes read." << endl;
      m_close( &fd, fp, &ifs );
      delete[] buf;
      exit( ERROR_READ );
    }
    if (blockseq == nblock && readbytes != blocksize) {
      cout << "[Thread " << tid << "] Last block was partially filled (" <<
          readbytes << " bytes)" << endl;
    }
  }
  cout << "[Thread " << tid << "] Successfully read " << blockseq - 1 <<
      " blocks starting from offset " << offset << " bytes." << endl;
  m_close( &fd, fp, &ifs );

  delete[] buf;
  return NULL;
}

void spawnThreads() {
  int32_t i, retval;
  if ( threadImpl == t_POSIX ) {
    pthread_t *threads = new pthread_t[nthread];
    for (i=0; i<nthread; ++i) {
      retval = pthread_create( &threads[i], NULL, threadedRead, (void *)( (int64_t) i ) );
      if ( retval ) {
        cerr << "Error creating thread " << i << "." << endl;
        delete[] threads;
        exit( ERROR_CREATE_THREAD );
      }
    }
    for (i=0; i<nthread; ++i) {
      pthread_join( threads[i], NULL );
    }
    delete[] threads;
  }
  else if ( threadImpl == t_CPP11 ) {
    thread *threads = new thread[nthread];
    for (i=0; i<nthread; ++i) {
      threads[i] = thread( threadedRead, (void *)( (int64_t) i ) );
    }
    for (i=0; i<nthread; ++i) {
      threads[i].join();
    }
    delete[] threads;
  }
}

int main( int argc, char *argv[] ) {
  processArgs(argc, argv);
  getFileLength();
  spawnThreads();
  return 0;
}
