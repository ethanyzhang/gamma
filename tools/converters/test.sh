#!/bin/bash
# Copyright (C) 2013-2018 Yiqun Zhang <contact@yzhang.io>
# All Rights Reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Check executable files
flist=("csv2binary" "tab2vert" "vert2tab")
for f in ${flist[@]}; do
  if ! [ -f ./$f ]; then
    echo -e "\033[1;91m[ERROR]\033[0m Cannot find executable file for $f, please compile first."
    exit -1
  fi
done

# Print test result
printResult() {
  if [ $# -ne 2 ]; then
    echo -e "\033[1;91m[ERROR]\033[0m Error calling \"printResult()\": expecting 2 arguments, but got $#."
  else
    if [ $1 -eq 0 ]; then
      echo -e "\033[0;92m[PASS]\033[0m $2."
    else
      echo -e "\033[1;91m[FAIL]\033[0m Wrong result for $2."
    fi
  fi
}

# Test csv2binary
./csv2binary test/tcase.csv test/.tcase.bin > /dev/null
diff test/tcase.bin test/.tcase.bin
printResult $? "csv2binary"
rm test/.tcase.bin

# Test tab2vert
./tab2vert test/tcase.csv test/.tcase.vert.csv > /dev/null
diff test/tcase.vert.csv test/.tcase.vert.csv
printResult $? "tab2vert"
rm test/.tcase.vert.csv

# Test vert2tab
./vert2tab test/tcase.vert.csv test/.tcase.csv > /dev/null
diff test/tcase.csv test/.tcase.csv
printResult $? "vert2tab"
rm test/.tcase.csv
