/*
 * Copyright (C) 2015 Yiqun Zhang <zhangyiqun9164@gmail.com>
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
 * @file LogicalLoad2D.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the Load2D SciDB operator.
 */

#include <query/Operator.h>
#include <fstream>
#define CHUNK_SIZE 10000
using namespace std;

namespace scidb {

class LogicalLoad2D : public LogicalOperator {
public:
  LogicalLoad2D(const string& logicalName, const string& alias):
    LogicalOperator(logicalName, alias) {
    // The parameters are the input binary file name, n and d.
    ADD_PARAM_CONSTANT(TID_STRING)
    ADD_PARAM_CONSTANT(TID_INT64)
    ADD_PARAM_CONSTANT(TID_INT64)
  }

  ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query) {
    string fname = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                        query, TID_STRING).getString();
    int64_t n = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                        query, TID_INT64).getInt64();
    int64_t d = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                        query, TID_INT64).getInt64();

    ifstream fin;
    fin.open(fname.c_str(), ios::in);
    if ( ! fin.is_open()) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANT_OPEN_FILE) << fname << "Cannot open file" << 0;
    }
    fin.close();

    if (n<1 || d<1) {
      throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
                               << "n and d have to be greater or equal to 1.";
    }

    Attributes outputAttributes;
    outputAttributes.push_back( AttributeDesc(0, "val", TID_DOUBLE, 0, 0) );
    outputAttributes = addEmptyTagAttribute(outputAttributes);

    Dimensions outputDims;
    int64_t chunkSize = n < CHUNK_SIZE ? n : CHUNK_SIZE;
    outputDims.push_back( DimensionDesc("i", 1, n, chunkSize, 0) ); 
    outputDims.push_back( DimensionDesc("j", 1, d, d, 0) ); 
    return ArrayDesc("Load2DdArray", outputAttributes, outputDims, defaultPartitioning());
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalLoad2D, "load2d");

} //namespace scidb
