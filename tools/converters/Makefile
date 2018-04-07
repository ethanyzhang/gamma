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

CXX = g++
RM = rm -f
CXXFLAGS = -Wall -std=c++11
TARGET = tab2vert vert2tab csv2binary

all: $(TARGET)

csv2binary: src/csv2binary.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

tab2vert: src/tab2vert.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

vert2tab: src/vert2tab.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	$(RM) $(TARGET)
