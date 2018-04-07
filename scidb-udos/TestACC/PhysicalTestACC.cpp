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
 * @file PhysicalTestACC.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the TestACC SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#define N 80000000
using namespace std;

double pi(bool usegpu);

namespace scidb {

class PhysicalTestACC : public PhysicalOperator {
public:
  
  PhysicalTestACC(string const& logicalName,
               string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }

  shared_ptr<Array> execute(vector<shared_ptr<Array>>& inputArrays, shared_ptr<Query> query) {
    bool usegpu = ((shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getBool();
    double p = pi(usegpu);
    shared_ptr<Array> outputArray(new MemArray(_schema, query));
    shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
    shared_ptr<ChunkIterator> outputChunkIter;
    Coordinates position(1, 1);
    position[0] = query->getInstanceID() + 1;
    outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
    Value valueToWrite;
    valueToWrite.setDouble(p);
    outputChunkIter->setPosition(position);
    outputChunkIter->writeItem(valueToWrite);
    outputChunkIter->flush();
    return outputArray;
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalTestACC, "TestACC", "PhysicalTestACC");

} //namespace scidb
