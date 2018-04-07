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
 
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
using namespace std;

void help() {
  cout << "Transform tabluar csv file to its vertical form." << endl;
  cout << "Usage: tab2vert [input] [output]" << endl;
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
  lineno = colno = 1;

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
    colno = 1;
    for (iter=inputLine.begin(); iter<=inputLine.end(); iter++) {
      if (iter == inputLine.end() || *iter == ',') {
        fout << lineno << "," << colno << "," << ss.str() << endl;
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
  fout.flush();
  fout.close();
  return 0;
}
