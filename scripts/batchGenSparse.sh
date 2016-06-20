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

n=(10000000)
d=(100 200 400 800)
den=(0.001 0.01 0.1 1)
for i in ${n[@]}; do
  strn=`short $i`
  for j in ${d[@]}; do
    for k in ${den[@]}; do
      strden=`echo "scale=0; $k*1000/1" | bc`
      name=`printf "n%sd%03dden%04d" $strn $j $strden`
      cmd="gensparse $name $i $j $k"
      echo -e "\033[0;32m$cmd\033[0m"
      $cmd
    done
  done
done
