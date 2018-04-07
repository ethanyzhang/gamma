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

if [ $# -ne 1 ]; then
  echo "Usage: run.sh [data file path]"
  echo "Example:"
  echo "run.sh \"hdfs://node1:54310/KDDn100Kd38_Y.csv\""
  echo "run.sh \"hdfs://node1:54310/KDDn001Md38_Y.csv\""
  echo "run.sh \"hdfs://node1:54310/KDDn010Md38_Y.csv\""
  echo "run.sh \"hdfs://node1:54310/KDDn100Md38_Y.csv\""
  exit -1;
fi
cd "$( dirname "${BASH_SOURCE[0]}" )"
spark-submit --class GammaSpark target/scala-2.10/gammaspark_2.10-1.0.jar $1
