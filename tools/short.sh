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

if [ $# -ne 1 ]; then
  echo "Get the short representation of a number."
  echo "Usage: short [number]"
  echo "Example: short 1000000"
  echo "Return value: 001M"
  exit
fi

value=$1
unit=""
if [ $1 -ge 1000000 ]; then
  value=`echo "$1/1000000" | bc`
  unit="M"
elif [ $1 -ge 1000 ]; then
  value=`echo "$1/1000" | bc`
  unit="K"
fi
printf "%03d%s\n" $value $unit
