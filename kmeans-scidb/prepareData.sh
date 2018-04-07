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
    mkdir -p $dataFolder
fi

if [ -z `which short` ]; then
    printError "Cannot find commad short."
fi

printInfo "Generating data sets..."
n=1000000 # 1M
d=8
# n = 1M and 10M
while [ $n -le 10000000 ]; do
    k=4
    while [ $k -le 16 ]; do
        fileName="$(printf "n%sd%02dk%02d" `short $n` $d $k)"
        fileNameForCenters="c-$fileName.csv"
        fileNameForXBinary="x-$fileName.bin"
        fileNameForXCSV="x-$fileName.csv"
        if [ -f $dataFolder/$fileNameForCenters -a -f $dataFolder/$fileNameForXBinary -a -f $dataFolder/$fileNameForXCSV ]; then
            printWarn "File $fileNameForCenters, $fileNameForXBinary, and $fileNameForXCSV already exist in $dataFolder, skip."
        else
            ./genX.R $n $d $k
            mv $fileNameForXBinary $fileNameForXCSV $fileNameForCenters $dataFolder
        fi
        let k=k*2
    done
    let n=n*10
done
