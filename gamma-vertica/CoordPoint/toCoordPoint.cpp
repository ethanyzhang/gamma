/*
 * Copyright (C) 2013-2016 Yiqun Zhang <zhangyiqun9164@gmail.com>
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Vertica.h"
using namespace Vertica;

class toCoordPoint : public ScalarFunction {
public:

  struct CoordPoint {
    vint i;
    vint j;
    vfloat v;
  };

  virtual void processBlock(ServerInterface &srvInterface,
                BlockReader &argReader,
                BlockWriter &resWriter) {
    try {
      // Basic error checking
      if (argReader.getNumCols() != 3)
        vt_report_error(0, "Function only accept 3 arguments, but %zu provided", 
                argReader.getNumCols());
      vsize cpSize = sizeof(struct CoordPoint);
      do {
        CoordPoint cp;
        cp.i = argReader.getIntRef(0);
        cp.j = argReader.getIntRef(1);
        cp.v = argReader.getFloatRef(2);

        resWriter.getStringRef().copy(reinterpret_cast<char *>(&cp), cpSize);
        resWriter.next();
      }
      while (argReader.next());
    }
    catch(std::exception& e) {
      // Standard exception. Quit.
      vt_report_error(0, "Exception while processing block: [%s]", e.what());
    }
  }
};

class toCoordPointFactory : public ScalarFunctionFactory
{
  // return an instance of toCoordPoint to perform the actual addition.
  virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
  { return vt_createFuncObject<toCoordPoint>(interface.allocator); }

  virtual void getPrototype(ServerInterface &interface,
                ColumnTypes &argTypes,
                ColumnTypes &returnType) {
    argTypes.addInt();
    argTypes.addInt();
    argTypes.addFloat();
    returnType.addLongVarbinary();
  }

  virtual void getReturnType(ServerInterface &srvInterface,
                 const SizedColumnTypes &argTypes,
                 SizedColumnTypes &returnType) {
    vsize cpSize = sizeof(vint)*2+sizeof(vfloat);
    returnType.addLongVarbinary(cpSize);
  }
};

RegisterFactory(toCoordPointFactory);
