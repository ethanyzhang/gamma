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

class extractCoordPointValue : public ScalarFunction {
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
      if (argReader.getNumCols() != 1)
        vt_report_error(0, "Function only accept 1 arguments, but %zu provided", 
                argReader.getNumCols());
      do {
        const CoordPoint* cp = reinterpret_cast<const CoordPoint *>(argReader.getStringRef(0).data());
        resWriter.setFloat(cp->v);
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

class extractCoordPointValueFactory : public ScalarFunctionFactory
{
  // return an instance of extractCoordPointValue to perform the actual addition.
  virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
  { return vt_createFuncObject<extractCoordPointValue>(interface.allocator); }

  virtual void getPrototype(ServerInterface &interface,
                ColumnTypes &argTypes,
                ColumnTypes &returnType) {
    argTypes.addLongVarbinary();
    returnType.addFloat();
  }
};

RegisterFactory(extractCoordPointValueFactory);
