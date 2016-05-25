#include <fstream>
#include <cstdint>
#include <iostream>
#include <sstream>
using namespace std;

void help() {
  cout << "Transform a coordinate matrix csv file to its tabluar form." << endl;
  cout << "Usage: vert2tab [file name]" << endl;
  exit(0);
}

int main(int argc, char *argv[]) {
  if (argc != 2) help();

  ifstream fin;
  string inputLine;
  string::iterator iter;
  stringstream inputBuffer;
  stringstream outputBuffer;
  int currLine = -1;
  int col, intValue;
  double doubleValue;

  fin.open(argv[1], ios::in);
  if (! fin.is_open()) {
    cerr << "[ERROR] Cannot open file " << argv[1] << endl;
    return -1;
  }

  while (getline(fin, inputLine)) {
    // There should be only 3 columns in each line.
    for (col=0, iter=inputLine.begin(); iter<=inputLine.end(); iter++) {
      if (iter == inputLine.end() || *iter == ',') {
        // the column value should be in inputBuffer.
        if (col == 0) {
          intValue = stoi(inputBuffer.str()); // row id
          if (currLine != intValue) {
            if (currLine != -1) {
              cout << outputBuffer.str().substr(0, outputBuffer.str().length()-1) << endl;
              outputBuffer.str("");
            }
            currLine = intValue;
          }
        }
        // ignore the second column.
        else if (col == 2) {
          doubleValue = stod(inputBuffer.str());
          outputBuffer << inputBuffer.str() << ",";
        }
        col++;
        inputBuffer.str("");
      }
      else {
        if (*iter != ' ')
          inputBuffer << *iter;
      }      
    }
  } // end while
  cout << outputBuffer.str().substr(0, outputBuffer.str().length()-1) << endl;
  fin.close();
  return 0;
}
