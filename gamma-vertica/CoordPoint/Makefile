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

SDK_HOME=/opt/vertica/sdk
CXX=g++
CXXFLAGS=-O3 -I $(SDK_HOME)/include -I HelperLibraries -g -Wall -Wno-unused-value -shared -fPIC

all: toCoordPoint.so extractCoordPointValue.so

toCoordPoint.so: toCoordPoint.cpp $(SDK_HOME)/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h
	$(CXX) $(CXXFLAGS) -o $@ toCoordPoint.cpp $(SDK_HOME)/include/Vertica.cpp

extractCoordPointValue.so: extractCoordPointValue.cpp $(SDK_HOME)/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h
	$(CXX) $(CXXFLAGS) -o $@ extractCoordPointValue.cpp $(SDK_HOME)/include/Vertica.cpp
