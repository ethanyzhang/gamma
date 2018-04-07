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

# This script is used to distribute the compiled library file to every
# SciDB server. 
# Servers need to have their IP addresses stored in the hosts file
# The servers should have their names following the format "nodeX"
# For example: node1, node2, ...
# Run this script in the operator's folder because the script get the name
# of the operator from the current working directory name.

if [ $# -ne 1 ]; then
  echo "Usage: distribute [node count]"
  echo "Example: distribute 10"
  exit
fi
opname=`pwd | sed "s/^.*\/\(.*$\)/\1/g"`
echo "Operator name is $opname."
if ! [ -f lib$opname.so ]; then
  echo "Cannot find library file lib$opname.so, exit."
  exit
fi
for id in `seq 1 1 $1`; do
  host=`printf "node%d" $id`
  scp lib$opname.so `whoami`@$host:$SCIDB_INSTALL_PATH/lib/scidb/plugins/
done
