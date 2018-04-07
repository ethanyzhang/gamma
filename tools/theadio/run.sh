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

printf "%7s  %7s  %7s  %7s  %9s  %s\n" "io" "thread" "n" "nthread" "blocksize" "time"
for io in "posix" "stdio" "fstream"; do
    for thread in "posix" "c++11"; do
        for n in "001M" "010M" "100M"; do
            for nthread in 1 2 4; do
                for blocksize in 1000 10000 100000; do
                    printf "%7s  %7s  %7s  %7s  %9s  " $io $thread $n $nthread $blocksize
                    t=$( { /usr/bin/time -f "%e" ./threadio "file=n${n}d100.dat;nthread=$nthread;thread=$thread;io=$io;blocksize=$blocksize" > /dev/null; } 2>&1 )
                    echo $t
                done
            done
        done
    done
done
