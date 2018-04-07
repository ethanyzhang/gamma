#!/bin/bash
# Copyright (C) 2013-2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
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

printInfo() {
  echo -e "\033[1;96m[INFO] \033[0m$1"
}

printWarn() {
  echo -e "\033[1;93m[WARN] \033[0m$1"
}

printError() {
  echo -e "\033[1;91m[ERROR] \033[0m$1"
}

dataFolder="$HOME/dataset"
if ! [ -d $dataFolder ]; then
  printError "Data folder $dataFolder does not exist."
  exit -1
fi

if [ -z `which iquery` ]; then
  printError "Cannot find commad iquery, please make sure SciDB is correctly installed and \$SCIDB_INSTALL_PATH is in \$PATH."
  exit -1
fi

if [ -z `which short` ]; then
  printError "Cannot find commad short."
  exit -1
fi

printInfo "Loading data sets..."
n=100000 # 100K
while [ $n -le 10000000 ]; do
    d=2
    while [ $d -le 16 ]; do
        k=2
        while [ $k -le 16 ]; do
            printInfo "n = $n, d = $d, k = $k:"
            fileNameForX="x-n`short $n`-d$d-k$k.bin"
            if ! [ -f $dataFolder/$fileNameForX ]; then
              printWarn "File $fileNameForX does not exist in $dataFolder, skip."
            else
              iquery -anq "store(load2d('$dataFolder/$fileNameForX', $n, $d), n`short $n`d${d}k$k);"
            fi
            let k=k*2            
        done
        let d=d*2
    done
    let n=n*10
done
printInfo "Data loading is complete."
