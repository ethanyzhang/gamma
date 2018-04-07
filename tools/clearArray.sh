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

if [ -z `which iquery` ]; then
  echo "ERROR: Cannot find command \"iquery\", please make sure SciDB is properly installed."
  exit -1
fi
echo -n "This will remove all the arrays in SciDB, sure? [y/n]: "
read -n 1 response
echo ""
if [ "$response" = "y" ]; then
  for arr in `iquery -aq "list('arrays')" | cut -d' ' -f2 | cut -d"'" -f2 | sed "1d"`; do
    iquery -aq "remove($arr);";
  done
fi