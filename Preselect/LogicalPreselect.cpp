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
 * @file LogicalPreselect.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the Preselect SciDB operator.
 */

#include <query/Operator.h>
using namespace std;

namespace scidb {

class LogicalPreselect : public LogicalOperator {
public:

  LogicalPreselect(const string& logicalName, const string& alias):
    LogicalOperator(logicalName, alias) {
    ADD_PARAM_INPUT()        // Original array
    ADD_PARAM_INPUT()         // Correlation array
    ADD_PARAM_CONSTANT(TID_INT64)  // d
  }

  ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query) {
    FILE *logFile;
    logFile = fopen("/home/scidb/prelog.log", "w");
    ///////////////////// Verify input arrays. ////////////////////////////////////

    ArrayDesc const& originalSchema = schemas[0];
    ArrayDesc const& corrSchema = schemas[1];
    fprintf(logFile, "original: %s\ncorr: %s\n", originalSchema.getName().c_str(), corrSchema.getName().c_str());

    Dimensions originalDims = originalSchema.getDimensions();
    Dimensions corrDims = corrSchema.getDimensions();
    // The original input array should have 2 dimensions: i and j.
    if(originalDims.size() != 2) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "Operator Preselect accepts an original array with exactly 2 dimensions.";
    }
    fprintf(logFile, "original has %ld dimensions, corr has %ld dimensions\n", originalDims.size(), corrDims.size());
    fflush(logFile);
    // The correlation input array should have 1 dimensions: i.
    if(corrDims.size() != 1) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "Operator Preselect accepts a correlation array with exactly 1 dimension.";
    }

    // The original input array should have only 1 attribute, and it should be in double type.
    if (originalSchema.getAttributes(true).size() != 1 ||
      originalSchema.getAttributes(true)[0].getType() != TID_DOUBLE) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "Operator Preselect accepts an original array with one attribute of type double";
    }

    // The correlation input array should have only 1 attribute, and it should be in double type.
    if (corrSchema.getAttributes(true).size() != 1 ||
      corrSchema.getAttributes(true)[0].getType() != TID_DOUBLE) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "Operator Preselect accepts a correlation array with one attribute of type double";
    }

    DimensionDesc originalDimsP = originalDims[1];
    DimensionDesc originalDimsN = originalDims[0];
    DimensionDesc corrDimsP = corrDims[0];
    // We assume the original input data set has p columns and an additional Y column.
    Coordinate p_original = originalDimsP.getCurrLength()-1;
    // Note the correlation array doesn't have Y column.
    Coordinate p_corr = corrDimsP.getCurrLength();
    Coordinate pStart = originalDimsP.getCurrStart(); 
    fprintf(logFile, "p_original = %ld, p_corr = %ld\n", p_original, p_corr);
    fprintf(logFile, "corr currEnd = %ld, corr currStart = %ld\n", corrDimsP.getCurrEnd(), corrDimsP.getCurrStart());
    fprintf(logFile, "original currLength = %ld, corr currLength = %ld\n", originalDimsP.getCurrLength(), corrDimsP.getCurrLength());
    fflush(logFile);
    // The number of dimension and the number of correlation values should match.
    if(p_original != p_corr) {
      fprintf(logFile, "p_original = %ld, p_corr = %ld\n", p_original, p_corr);
      fflush(logFile);
      fclose(logFile);
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "p in original array and correlation array don't match.";
    }

    if(originalDimsP.getChunkInterval() < p_original+1) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "Chunk size of the column dimension in original input array must be no less than p+1.";
    }

    if(corrDimsP.getChunkInterval() < p_corr) {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
          << "Chunk size of the column dimension in correlation input array must be no less than p+1.";
    }

    // The output array has only one attribute which is exactly the same as the input.
    Attributes outputAttributes;
    outputAttributes.push_back( originalSchema.getAttributes(true)[0] );
    outputAttributes = addEmptyTagAttribute(outputAttributes);
    fprintf(logFile, "# of parameters: %ld\n", _parameters.size());
    Coordinate d = evaluate(((boost::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                        query, TID_INT64).getInt64();
    fprintf(logFile, "d = %ld\npStart=%ld, pStart+d=%ld\n", d, pStart, pStart+d);
    DimensionDesc j("j", pStart, pStart + d, d+1, 0);
    fflush(logFile);
    Dimensions outputDims;
    outputDims.push_back(originalDimsN); 
    outputDims.push_back(j);
    return ArrayDesc("preselect_" + originalSchema.getName(), outputAttributes, outputDims, defaultPartitioning());
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalPreselect, "preselect");

} //namespace scidb
