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

/*
 * This Gamma operator is proposed in the paper:
 * The Gamma Operator for Big Data Summarization on an Array DBMS
 * Carlos Ordonez, Yiqun Zhang, Wellington Cabrera 
 * Journal of Machine Learning Research (JMLR): Workshop and Conference Proceedings (BigMine 2014) 
 *
 * Please cite the paper above if you need to use this code in your research work.
 */

#include "Vertica.h"
#include <time.h>
#define D_MAX 200
#define COL_i 0
#define COL_j 1
#define COL_v 2
#define COORD(i,j) (i*(i+1)/2+j)

using namespace Vertica;
using namespace std;

class DenseGammaBuilder : public TransformFunction {
public:
  virtual void processPartition(ServerInterface &srvInterface, PartitionReader &inputReader, PartitionWriter &outputWriter) {
    try {
      vint d = srvInterface.getParamReader().getIntRef("d");
      vint GammaBufSize = (d+2)*(d+1)/2;
      vfloat GammaBuf[GammaBufSize];
      vfloat z_i[d+1];
      vint i, j;
      for (i=0; i<GammaBufSize; i++) {
        GammaBuf[i] = 0.0;
      }
      z_i[0] = 1.0;

      const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
      vector<size_t> argCols;
      inTypes.getArgumentColumns(argCols);
      vint currRow = inputReader.getIntRef(argCols.at(COL_i));
      vint currCol = 1;
      do {
        vint row = inputReader.getIntRef(argCols.at(COL_i));
        vint col = inputReader.getIntRef(argCols.at(COL_j));
        vfloat val = inputReader.getFloatRef(argCols.at(COL_v));
        // Entering the new row
        if (row != currRow) {
          for (i=0; i<currCol; i++) {
            for (j=0; j<=i; j++) {
              GammaBuf[COORD(i, j)] += z_i[i] * z_i[j];
            }
          }
          currRow = row;
          currCol = 1;
        }
        if (currCol != col) {
          vt_report_error(0, "currRow=%d, currCol=%d, col=%d",
                  (int)currRow, (int)currCol, (int)col);
        }
        // srvInterface.log("{%d, %d} %lf", (int)row, (int)col, (double)val);
        z_i[currCol] = val;
        currCol++;
      } while (inputReader.next());
      for (i=0; i<currCol; i++) {
        for (j=0; j<=i; j++) {
          GammaBuf[COORD(i, j)] += z_i[i] * z_i[j];
        }
      }
      char* GammaBufPtr = reinterpret_cast<char*>(GammaBuf);
      outputWriter.getStringRef(0).copy(GammaBufPtr, GammaBufSize*sizeof(vfloat));
      outputWriter.next();
    }
    catch (bad_alloc &ba) {
      vt_report_error(1, "Couldn't allocate memory :[%s]", ba.what());
    }
    catch (exception &e) {
      // Standard exception. Quit.
      vt_report_error(0, "Exception while processing partition: [%s]", e.what());
    }
  }
};

class DenseGammaFactory;

class DenseGammaCombiner : public TransformFunction {
public:
  virtual void processPartition(ServerInterface &srvInterface, PartitionReader &inputReader, PartitionWriter &outputWriter)
  {
    try {
      vint d = srvInterface.getParamReader().getIntRef("d");
      vint GammaBufSize = (d+2)*(d+1)/2;
      vfloat GammaBuf[GammaBufSize];
      vint i, j;
      for (i=0; i<GammaBufSize; i++) {
        GammaBuf[i] = 0.0;
      }
      // Merge Gamma from all partitions.
      do {
        const char* localGammaBufPtr = inputReader.getStringRef(0).data();
        const vfloat* localGammaBuf = reinterpret_cast<const vfloat*>(localGammaBufPtr);
        for (i=0; i<GammaBufSize; i++) {
          GammaBuf[i] += localGammaBuf[i];
        }
      } while (inputReader.next());
      // Write result!
      for (i=0; i<d+1; i++) {
        for (j=0; j<d+1; j++) {
          outputWriter.setInt(COL_i, i+1);
          outputWriter.setInt(COL_j, j+1);
          if(i>=j) {
            outputWriter.setFloat(COL_v, GammaBuf[COORD(i,j)]);
          }
          else {
            outputWriter.setFloat(COL_v, GammaBuf[COORD(j,i)]);
          }
          outputWriter.next();
        }
      }
    }
    catch (bad_alloc &ba) {
      vt_report_error(1, "Couldn't allocate memory :[%s]", ba.what());
    }
    catch(exception& e) {
      // Standard exception. Quit.
      vt_report_error(0, "Exception while processing partition: [%s]", e.what());
    }
  }
};

class DenseGammaFactory : public MultiPhaseTransformFunctionFactory {
public:
  class BuildingPhase : public TransformFunctionPhase {
    virtual void getReturnType(ServerInterface &srvInterface,
                 const SizedColumnTypes &inputTypes,
                 SizedColumnTypes &outputTypes) {
      // Expected input:
      //   (i INTEGER, j INTEGER, v FLOAT)
      vector<size_t> argCols;
      inputTypes.getArgumentColumns(argCols);
      vector<size_t> pByCols;
      inputTypes.getPartitionByColumns(pByCols);
      vector<size_t> oByCols;
      inputTypes.getOrderByColumns(oByCols);

      // if (argCols.size() != 3 || pByCols.size() != 1 || oByCols.size() != 2 ||
      //   !inputTypes.getColumnType(argCols.at(COL_i)).isInt() ||
      //   !inputTypes.getColumnType(argCols.at(COL_j)).isInt() ||
      //   !inputTypes.getColumnType(argCols.at(COL_v)).isFloat() ||
      //   !inputTypes.getColumnType(oByCols.at(COL_i)).isInt() ||
      //   !inputTypes.getColumnType(oByCols.at(COL_j)).isInt())
      //   vt_report_error(0, "Function expects three arguments (i INTEGER, j INTEGER, v FLOAT) with "
      //           "analytic clause OVER(PBY mod(i,4) OBY i, j)");

      // Output of this phase is:
      //   (partial_gamma LONG VARBINARY)
      // One row per partition.
      outputTypes.addLongVarbinary(D_MAX*(D_MAX+1)*4, "partital_gamma");
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<DenseGammaBuilder>(srvInterface.allocator); }
  };

  class CombiningPhase : public TransformFunctionPhase {
    virtual void getReturnType(ServerInterface &srvInterface,
                 const SizedColumnTypes &inputTypes,
                 SizedColumnTypes &outputTypes) {
      // Expected input:
      //   (partial_gamma LONG VARBINARY)
      vector<size_t> argCols;
      inputTypes.getArgumentColumns(argCols);

      if (argCols.size() != 1 ||
          !inputTypes.getColumnType(argCols.at(0)).isLongVarbinary())
        vt_report_error(0, "Function expects one argument: "
                           "(partial_gamma LONG VARBINARY).");

      // Output of this phase is:
      //   (i INTEGER, j INTEGER, v FLOAT).
      outputTypes.addIntOrderColumn("i");
      outputTypes.addIntOrderColumn("j");
      outputTypes.addFloat("v");
    }
     
     virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
     { return vt_createFuncObject<DenseGammaCombiner>(srvInterface.allocator); }
   };

   BuildingPhase buildingPh;
   CombiningPhase combiningPh;

  virtual void getPhases(ServerInterface &srvInterface, std::vector<TransformFunctionPhase *> &phases) {
    buildingPh.setPrepass(); // Process data wherever they're originally stored.
    phases.push_back(&buildingPh);
    phases.push_back(&combiningPh);
  }

  virtual void getPrototype(ServerInterface &srvInterface,
               ColumnTypes &argTypes,
               ColumnTypes &returnType) {
    // Expected input: (i INTEGER, j INTEGER, v FLOAT).
    argTypes.addInt();
    argTypes.addInt();
    argTypes.addFloat();

    // Output is: (i INTEGER, j INTEGER, v FLOAT)
    returnType.addInt();
    returnType.addInt();
    returnType.addFloat();
  }

  virtual void getParameterType(ServerInterface &srvInterface,
                    SizedColumnTypes &parameterTypes) {
    parameterTypes.addInt("d");
  }
};

RegisterFactory(DenseGammaFactory);
