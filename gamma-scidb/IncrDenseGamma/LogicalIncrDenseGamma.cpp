/*
 * Copyright (C) 2013-2017 Yiqun Zhang <zhangyiqun9164@gmail.com>
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

/*
 * @file LogicalIncrDenseGamma.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the IncrDenseGamma SciDB operator.
 */

#include <query/Operator.h>
using namespace std;

namespace scidb {

class LogicalIncrDenseGamma : public LogicalOperator {
public:

  LogicalIncrDenseGamma(const string& logicalName, const string& alias):
    LogicalOperator(logicalName, alias) {
    ADD_PARAM_INPUT()               // array name
    ADD_PARAM_CONSTANT("int64");    // chunk serial number to continue, start from 0
  }
  
  ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query) {
    ArrayDesc const& inputSchema = schemas[0];
    Dimensions inputDims = inputSchema.getDimensions();
    
    // The input array should have 2 dimensions: i and j.
    if(inputDims.size() != 2) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Operator Gamma accepts an array with exactly 2 dimensions.";
    }
    
    // The input array should have only 1 attribute, and it should be in double type.
    if (inputSchema.getAttributes(true).size() != 1 ||
      inputSchema.getAttributes(true)[0].getType() != TID_DOUBLE) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Operator Gamma accepts an array with one attribute of type double";
    }
    
    // The output array has only one attribute which is exactly the same as the input.
    Attributes outputAttributes;
    outputAttributes.push_back( inputSchema.getAttributes(true)[0] );
    outputAttributes = addEmptyTagAttribute(outputAttributes);
    
    DimensionDesc dimsD = inputDims[1];
    // We assume the input data set has d columns and an additional Y columns.
    Coordinate d = dimsD.getCurrEnd() - dimsD.getCurrStart();
    
    if(dimsD.getChunkInterval() < d+1) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << "Chunk size of the column dimension must be no less than d+1.";
    }
    
    DimensionDesc i("i", 1, d+2, d+2, 0);
    DimensionDesc j("j", 1, d+2, d+2, 0);
    Dimensions outputDims;
    outputDims.push_back(i); 
    outputDims.push_back(j); 
#ifdef SCIDB16
    return ArrayDesc("IncrDenseGamma_" + inputSchema.getName(),
                     outputAttributes, outputDims,
                     defaultPartitioning(),
                     query->getDefaultArrayResidency());
#else
    return ArrayDesc("IncrDenseGamma_" + inputSchema.getName(),
                     outputAttributes, outputDims,
                     defaultPartitioning());
#endif
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalIncrDenseGamma, "IncrDenseGamma");

} //namespace scidb
