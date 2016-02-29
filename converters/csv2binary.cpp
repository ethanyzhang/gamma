#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#define BUFFER_SIZE 1024
using namespace std;

void help() {
  cout << "Usage: csv2binary [input] [output]" << endl;
  exit(0);
}

int main(int argc, char *argv[]) {
  if (argc != 3) help();

  ifstream fin;
  ofstream fout;
  string inputLine;
  string::iterator iter;
  stringstream ss;
  int64_t lineno, colno;
  double buffer[BUFFER_SIZE];
  int64_t bufferedCount = 0;
  lineno = colno = 0;
  fin.open(argv[1], ios::in);
  fout.open(argv[2], ios::binary);
  if ( ! fin.is_open()) {
    cerr << "[ERROR] cannot open file " << argv[1] << endl;
    return -1;
  }
  if ( ! fout.is_open()) {
    cerr << "[ERROR] cannot open file " << argv[2] << endl;
    return -1;
  }

  while (getline(fin, inputLine)) {
    colno = 0;
    for (iter=inputLine.begin(); iter<=inputLine.end(); iter++) {
      if (iter == inputLine.end() || *iter == ',') {
        buffer[bufferedCount++] = stod(ss.str());
        ss.str("");
        colno++;
        if (bufferedCount == BUFFER_SIZE) {
          fout.write((char *)buffer, bufferedCount * sizeof(double));
          bufferedCount = 0;
        }
      }
      else {
        ss << *iter;
      }
    }
    lineno++;
  }
  if (bufferedCount != 0) {
    fout.write((char *)buffer, bufferedCount * sizeof(double));
  }
  cout << "Conversion complete: " << lineno << " lines and " << colno << " columns." << endl;
  fin.close();
  fout.close();
  return 0;
}