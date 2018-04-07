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

#include <fstream>
#include <cstdint>
#include <iostream>
#include <sstream>
using namespace std;

void help() {
  cout << "Transform a coordinate matrix csv file to its tabluar form." << endl;
  cout << "Usage: vert2tab [input] [output]" << endl;
  exit(0);
}

int main(int argc, char *argv[]) {
  if (argc != 3) help();

  ifstream fin;
  ofstream fout;
  string inputLine;
  string::iterator iter;
  stringstream inputBuffer;
  stringstream outputBuffer;
  int currLine = -1;
  int col, intValue;

  fin.open(argv[1], ios::in);
  if (! fin.is_open()) {
    cerr << "[ERROR] Cannot open file " << argv[1] << endl;
    return -1;
  }
  fout.open(argv[2], ios::out);
  if (! fout.is_open()) {
    cerr << "[ERROR] Cannot open file " << argv[2] << endl;
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
              fout << outputBuffer.str().substr(0, outputBuffer.str().length()-1) << endl;
              outputBuffer.str("");
            }
            currLine = intValue;
          }
        }
        // ignore the second column.
        else if (col == 2) {
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
  fout << outputBuffer.str().substr(0, outputBuffer.str().length()-1) << endl;
  fin.close();
  fout.flush();
  fout.close();
  return 0;
}
