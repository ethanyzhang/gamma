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

#include <query/Operator.h>
#include <query/Network.h>
#include <query/ParsingContext.h>
// We need quick sort.
#include <stdlib.h>
#include <math.h>
#include <sstream>
#include "FilterArray.h"
namespace scidb
{
	
struct correlation
{
	int id;
	double corr;
};

class PhysicalPreselect : public PhysicalOperator
{
public:
	
    PhysicalPreselect(string const& logicalName,
                           string const& physicalName,
                           Parameters const& parameters,
                           ArrayDesc const& schema):
    	PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

	static int comp(const void* p1, const void* p2)
	{
		correlation* arr1 = (correlation*)p1;
		correlation* arr2 = (correlation*)p2;
		if(arr1->corr > arr2->corr)
		{
			return -1;
		}
		else if(arr1->corr == arr2->corr)
		{
			return 0;
		}
		else
		{
			return 1;
		}
	}

    shared_ptr< Array > execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
    	
    	// I maintain the log of the operator in a local file named after Correlation_N.log, N is the instance ID.
    	stringstream logFileName;
    	logFileName << "/home/scidb/preselect_" << query->getInstanceID() << ".log";
    	FILE *logFile;
    	logFile = fopen(logFileName.str().c_str(), "w");
    	
        shared_ptr<Array> originalArray = inputArrays[0];
        shared_ptr<Array> correlationArray = inputArrays[1];
        
        ArrayDesc originalSchema = originalArray->getArrayDesc();
        ArrayDesc corrSchema = correlationArray->getArrayDesc();
        
        Dimensions originalDims = originalSchema.getDimensions();
        Dimensions corrDims = corrSchema.getDimensions();
        DimensionDesc originalDimsP = originalDims[1];
        DimensionDesc corrDimsP = corrDims[0];
        // Note the correlation array doesn't have Y column.
        Coordinate p = corrDimsP.getCurrLength();
        fprintf(logFile, "p = %ld\n # of chunk = %ld\n", p, corrSchema.getNumberOfChunks());
        fflush(logFile);
        shared_ptr<ConstArrayIterator> corrArrayIter = correlationArray->getIterator(0);
        if(! corrArrayIter->end() )
        {
        	correlation *corr = new correlation[p];
	        // The correlation array will always have only 1 chunk (we designed correlation array like this), so no loops here.
	        shared_ptr<ConstChunkIterator> corrChunkIter = corrArrayIter->getChunk().getConstIterator();
			for(Coordinate i=0; i<p; ++i)
			{
				corr[i].id = i+1;
				corr[i].corr = corrChunkIter->getItem().getDouble();
				//fprintf(logFile, "%d, %f\n", corr[i].id, corr[i].corr);
				++(*corrChunkIter);
			}
			//fflush(logFile);
			qsort(corr, p, sizeof(correlation), &comp);
			for(Coordinate i=0; i<p; ++i)
			{
				fprintf(logFile, "%d, %f\n", corr[i].id, corr[i].corr);
			}
			fflush(logFile);
			
			Coordinate d = ((boost::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt64();
	        fprintf(logFile, "d=%ld\n", d);
	        stringstream ss;
	        vector<string> names;
	        names.push_back("j");
	        vector<TypeId> types;
	        types.push_back(TID_INT64);
	        for(Coordinate i=0; i<d; ++i)
	        {
	        	ss << "j=" << corr[i].id << " or ";
	        }
	        ss << "j=" << p+1;
	        fprintf(logFile, "%s\n", ss.str().c_str());
	        fflush(logFile);
	        Expression e;
        	e.compile(ss.str(), names, types);
        	fclose(logFile);
        	boost::shared_ptr<scidb::Query> emptyQuery;
	        return boost::shared_ptr<Array>(new FilterArray(_schema, inputArrays[0], boost::make_shared<Expression>(e), emptyQuery, _tileMode));
        }
        else
        {
        	shared_ptr<Array> outputArray(new MemArray(_schema, query));
        	fclose(logFile);
        	return outputArray;
        }
        
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalPreselect, "preselect", "PhysicalPreselect");

} //namespace scidb
