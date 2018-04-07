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

items=(
  rs
  scidbtime
  removeRarrays
  gensparse
  distribute
)

# Uninstall
if [ "$1" == "-u" ]; then
  echo -e "\033[0;93mUninstall mode:\033[0m"
  for item in ${items[@]}; do
    if [ -L /usr/local/bin/$item ]; then
      echo -e "\033[0;93m[REMOVE]\033[0m $item"
      sudo rm /usr/local/bin/$item
    else
      echo -e "\033[0;93m[REMOVE]\033[0m \033[1;97m$item\033[0m does not exist in /usr/local/bin"
    fi
  done
  exit 0
fi

# Install
for item in ${items[@]}; do
  if ! [ -x ./$item.sh ]; then
    echo -e "\033[1;91m[SKIP]\033[0m Cannot find executable file $u.sh"
  else
    if [ -L /usr/local/bin/$item ]; then
      echo -e "\033[0;93m[REMOVE]\033[0m Link to command $item already exists."
      sudo rm /usr/local/bin/$item
    fi
    cmd="ln -s $PWD/$item.sh /usr/local/bin/$item"
    echo -e "\033[0;92m[INSTALL]\033[0m $cmd"
    sudo $cmd
  fi
done;
echo "To uninstall, run ${BASH_SOURCE[0]} -u"
