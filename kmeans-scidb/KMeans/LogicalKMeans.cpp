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
 * @file LogicalKMeans.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the KMeans SciDB operator.
 */

#include <query/Operator.h>
using namespace std;

namespace scidb {

class LogicalKMeans : public LogicalOperator {
public:

  LogicalKMeans(const string& logicalName, const string& alias):
    LogicalOperator(logicalName, alias) {
    ADD_PARAM_INPUT()
    ADD_PARAM_CONSTANT(TID_INT32) // k
    ADD_PARAM_CONSTANT(TID_INT32) // max iteration
    ADD_PARAM_VARIES()            // config string
  }

  vector<shared_ptr<OperatorParamPlaceholder>> nextVaryParamPlaceholder(const vector< ArrayDesc> &schemas) {
    // The user could choose to give config string.
    vector<shared_ptr<OperatorParamPlaceholder>> res;
    res.push_back(END_OF_VARIES_PARAMS());
    if (_parameters.size() == 2) {
      res.push_back(PARAM_CONSTANT(TID_STRING));
    }
    return res;
  }
  
  ArrayDesc inferSchema(vector<ArrayDesc> schemas, shared_ptr<Query> query) {
    ArrayDesc const& inputSchema = schemas[0];
    Dimensions inputDims = inputSchema.getDimensions();
    // The input array should have 2 dimensions: i and j.
    if (inputDims.size() != 2) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Operator kmeans accepts an array with exactly 2 dimensions.";
    }
    // The input array should have only 1 attribute, and it should be in double type.
    if (inputSchema.getAttributes(true).size() != 1 ||
      inputSchema.getAttributes(true)[0].getType() != TID_DOUBLE) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Operator kmeans accepts an array with one attribute of type double";
    }
    DimensionDesc dimsN = inputDims[0];
    DimensionDesc dimsD = inputDims[1];
    Coordinate n = dimsN.getCurrEnd() - dimsN.getCurrStart() + 1;
    Coordinate d = dimsD.getCurrEnd() - dimsD.getCurrStart() + 1;
    int32_t k = evaluate(((shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                              query, TID_INT32).getInt32();
    int32_t imax = evaluate(((shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                              query, TID_INT32).getInt32();
    if (k<=0 || k>n) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "k needs to be greater than 0 and less than n.";
    }
    if (imax <= 0) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Maximum number of iterations must be greater than 0.";
    }
    if(dimsD.getChunkInterval() != d) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Chunk size of the column dimension must be d.";
    }
    // The output array has only one attribute which is exactly the same as the input.
    Attributes outputAttributes;
    outputAttributes.push_back( AttributeDesc(0, "val", TID_DOUBLE, 0, 0) );
    outputAttributes = addEmptyTagAttribute(outputAttributes);
    Dimensions outputDims;
    outputDims.push_back( DimensionDesc("i", 1, k, k, 0) );
    outputDims.push_back( DimensionDesc("j", 1, 2*d+2, 2*d+2, 0) ); // W: 1, C:d, R:d, average distance
    return ArrayDesc("KMeans_" + inputSchema.getName(), outputAttributes, outputDims, defaultPartitioning());
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalKMeans, "KMeans");

} //namespace scidb
