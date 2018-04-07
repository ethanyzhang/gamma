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
 * @file LogicalTestACC.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the TestACC SciDB operator.
 */

#include <query/Operator.h>
using namespace std;

namespace scidb {

class LogicalTestACC : public LogicalOperator {
public:

  LogicalTestACC(const string& logicalName, const string& alias):
    LogicalOperator(logicalName, alias) {
      ADD_PARAM_CONSTANT(TID_BOOL)
  }
  
  ArrayDesc inferSchema(vector<ArrayDesc> schemas, shared_ptr<Query> query) {
    Attributes outputAttributes;
    outputAttributes.push_back( AttributeDesc(0, "val", TID_DOUBLE, 0, 0) );
    outputAttributes = addEmptyTagAttribute(outputAttributes);
    int32_t N = query->getInstancesCount();
    DimensionDesc i("i", 1, N, 1, 0);
    Dimensions outputDims;
    outputDims.push_back(i);
    return ArrayDesc("TestACC", outputAttributes, outputDims, defaultPartitioning());
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalTestACC, "TestACC");

} //namespace scidb
