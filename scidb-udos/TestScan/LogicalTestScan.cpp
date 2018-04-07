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
 * @file LogicalTestScan.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the TestScan SciDB operator.
 */

#include <query/Operator.h>
using namespace std;

namespace scidb {

class LogicalTestScan : public LogicalOperator {
public:

  LogicalTestScan(const string& logicalName, const string& alias):
    LogicalOperator(logicalName, alias) {
      ADD_PARAM_INPUT()
      ADD_PARAM_CONSTANT(TID_BOOL)
  }
  
  ArrayDesc inferSchema(vector<ArrayDesc> schemas, shared_ptr<Query> query) {
    ArrayDesc inputSchema = schemas[0];
    Dimensions inputDims = inputSchema.getDimensions();
    DimensionDesc dimsD = inputDims[1];
    Coordinate d = dimsD.getCurrEnd() - dimsD.getCurrStart() + 1;
    // The input array should have 2 dimensions: i and j.
    if (inputDims.size() != 2) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Operator TestScan accepts an array with exactly 2 dimensions.";
    }
    // The input array should have only 1 attribute, and it should be in double type.
    if (inputSchema.getAttributes(true).size() != 1 ||
      inputSchema.getAttributes(true)[0].getType() != TID_DOUBLE) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Operator TestScan accepts an array with one attribute of type double";
    }
    if(dimsD.getChunkInterval() < d) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Chunk size of the column dimension must be no less than d.";
    }
    return inputSchema;
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalTestScan, "TestScan");

} //namespace scidb
