#!/bin/bash
# Copyright (C) 2013-2017 Yiqun Zhang <contact@yzhang.io>
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

if [ $# -eq 1 ]; then
    mkdir -p $HOME/scidbdata
    echo "password" | $SCIDB_SOURCE_PATH/deployment/deploy.sh scidb_prepare `whoami` "" local1$1 local1$1 local1$1 $HOME/scidbdata/local1$1 $1 default 0 default 127.0.0.1
else
    echo "Usage: $0 [number of instances]"
fi
