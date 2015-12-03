# Copyright (C) 2015 Yiqun Zhang <zhangyiqun9164@gmail.com>
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

BOOST_LOCATION=/opt/scidb/$(SCIDB_VER)/3rdparty/boost/include

OP_NAME=Preselect

CFLAGS=-std=c++11 -pedantic -W -Wextra -Wall -Wno-strict-aliasing -Wno-long-long -Wno-unused-parameter -Wno-variadic-macros -fPIC -D__STDC_FORMAT_MACROS -Wno-system-headers -isystem -O3 -g -DNDEBUG -ggdb3  -D__STDC_LIMIT_MACROS
INC=-I. -DPROJECT_ROOT="\"$(SCIDB_SOURCE_PATH)\"" -isystem"$(SCIDB_SOURCE_PATH)/include" -isystem"$(BOOST_LOCATION)"
LIBS=-L"$(SCIDB_SOURCE_PATH)/lib" -shared -Wl,-soname,lib$(OP_NAME).so -L. -lm

all:
	@if test -z "$(SCIDB_SOURCE_PATH)"; then echo "[ERROR] SCIDB_SOURCE_PATH is not set. Try:\n\n\tmake SCIDB_SOURCE_PATH=<PATH TO SCIDB TRUNK>\n"; exit 1; fi 

	$(CXX) $(CFLAGS) $(INC) -o Logical$(OP_NAME).cpp.o -c Logical$(OP_NAME).cpp
	$(CXX) $(CFLAGS) $(INC) -o Physical$(OP_NAME).cpp.o -c Physical$(OP_NAME).cpp

	$(CXX) $(CFLAGS) $(INC) -o lib$(OP_NAME).so \
	                           Logical$(OP_NAME).cpp.o \
	                           Physical$(OP_NAME).cpp.o \
	                           $(LIBS)

debug: CFLAGS += -D DEBUG
debug: all

clean:
	rm -f *.o *.so

install:
	@if test -z "$(SCIDB_VER)"; then echo "[ERROR] SCIDB_VER is not set. Try:\n\n\tmake install SCIDB_VER=<SCIDB VERSION>\n"; exit 1; fi
	cp *.so /opt/scidb/$(SCIDB_VER)/lib/scidb/plugins/
	iquery -aq "load_library('$(OP_NAME)');"

uninstall:
	@if test -z "$(SCIDB_VER)"; then echo "[ERROR] SCIDB_VER is not set. Try:\n\n\tmake install SCIDB_VER=<SCIDB VERSION>\n"; exit 1; fi
	iquery -aq "unload_library('$(OP_NAME)');"
	rm -f /opt/scidb/$(SCIDB_VER)/lib/scidb/plugins/lib$(OP_NAME).so
