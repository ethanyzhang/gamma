#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#include <vector>
using namespace std;

void help() {
  cout << "Transform tabluar csv file to its vertical form." << endl;
  cout << "Usage: tab2verti [file name]" << endl;
  exit(0);
}

int main(int argc, char *argv[]) {
  if (argc != 2) help();

  ifstream fin;
  string inputLine;
  string::iterator iter;
  stringstream ss;
  int64_t lineno, colno;
  lineno = colno = 1;

  fin.open(argv[1], ios::in);
  if (! fin.is_open()) {
    cerr << "[ERROR] Cannot open file " << argv[1] << endl;
    return -1;
  }

  while (getline(fin, inputLine)) {
    colno = 1;
    for (iter=inputLine.begin(); iter<=inputLine.end(); iter++) {
      if (iter == inputLine.end() || *iter == ',') {
        cout << lineno << "," << colno << "," << ss.str() << endl;
        ss.str("");
        colno++;
      }
      else {
        if (*iter != ' ')
          ss << *iter;
      }      
    }
    lineno++;
  }

  fin.close();
  return 0;
}
