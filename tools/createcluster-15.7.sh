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

if [ $# -ne 2 ]; then
  echo "Usage: ${BASH_SOURCE[0]} [node count] [instance per node]"
  echo "Example: ${BASH_SOURCE[0]} 10 2"
  exit
fi

user=`whoami`
ip=`hostname --ip-address`
echo "The IP address of this server is $ip."
former=`echo $ip | sed "s/\(^.*\.\).*$/\1/g"`
latter=`echo $ip | sed "s/^.*\.\(.*$\)/\1/g"`
# Remove previous ip addresses for node%d
cat /etc/hosts | grep -v "\([0-9]\{,3\}\.\)\{2\}[0-9]\{,3\} node[0-9]\{,3\}" | sudo tee /etc/hosts > /dev/null
for i in `seq 1 1 $1`; do
  printf "%s%d node%d\n" $former `expr $latter + $i - 1` $i | sudo tee -a /etc/hosts
done
sudo vim /etc/hosts

dbname=`printf "n%03di%02d" $1 $2`
echo -e "\033[0;32mPlease wait while we are distributing the new hosts file...\033[0m"
hostlist=""
for id in `seq 1 1 $1`; do
  host=`printf "node%d" $id`
  hostlist="$hostlist $host"
  echo "    $host"
  ssh $user@$host "sudo cp ~/.ssh/* /root/.ssh/" > /dev/null 2>&1
  ssh root@$host "scp root@${ip}:/etc/hosts /etc/hosts" > /dev/null 2>&1 &
done
for job in `jobs -p`; do
    wait $job
done

echo -e "\033[0;32mPreparing SciDB server...\033[0m"
echo "scidb" | $SCIDB_SOURCE_PATH/deployment/deploy.sh scidb_prepare $user "" $dbname $dbname $dbname /home/$user/scidbdata/$dbname $2 default 1 $hostlist
scidb.py startall $dbname

echo -e "\n\033[0;32mConfiguration complete.\033[0m"
