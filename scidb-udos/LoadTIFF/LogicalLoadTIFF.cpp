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
 * @file LogicalLoadTIFF.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Logical part of the LoadTIFF SciDB operator.
 */

#include <query/Operator.h>
#include <tiffio.h>
using namespace std;

namespace scidb {

class LogicalLoadTIFF : public LogicalOperator {
public:
  LogicalLoadTIFF(const string& logicalName, const string& alias):
      LogicalOperator(logicalName, alias) {
    // The first parameter should be the file name.
    ADD_PARAM_CONSTANT(TID_STRING)
    // There can be two more parameters after the file name, the height and the width {h, w}.
    // Those two parameters are used to specify a sub-range of the image to be imported.
    // A sub-range is a rectangular area from (0,0) to (h,w).
    ADD_PARAM_VARIES()
  }

  vector< shared_ptr< OperatorParamPlaceholder>> nextVaryParamPlaceholder(const vector< ArrayDesc> &schemas) {
    vector< shared_ptr< OperatorParamPlaceholder>> res;
    if (_parameters.size() == 3) {
      // Already got (fname, h, w), so we are done here.
      res.push_back(END_OF_VARIES_PARAMS());
    }
    else if (_parameters.size() == 2) {
      // Already got (fname, h), still need a parameter w.
      res.push_back(PARAM_CONSTANT(TID_UINT32));
    }
    else {
      // Only got fname, we can stop here (import the whole image)
      // or continue to accept height and width.
      res.push_back(END_OF_VARIES_PARAMS());
      res.push_back(PARAM_CONSTANT(TID_UINT32));
    }
    return res;
  }
    
  ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query) {
    // In this function we should check if the file is valid, if so, give out the array schema
    // according to the image height and width.
    string tiffName = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                                              query, TID_STRING).getString();
    Dimensions outputDims;
    Attributes outputAttributes;
    TIFF* tif = TIFFOpen(tiffName.c_str(), "r");
    if (tif) {
      uint32 originalWidth, originalHeight;
      uint32 importWidth, importHeight;
      TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, &originalWidth);
      TIFFGetField(tif, TIFFTAG_IMAGELENGTH, &originalHeight);
      importWidth = originalWidth;
      importHeight = originalHeight;
      TIFFClose(tif);
      outputAttributes.push_back( AttributeDesc(0, "val", TID_INT64, 0, 0) );
      outputAttributes = addEmptyTagAttribute(outputAttributes);
      
      // If the operator is configured to import a sub-range of the image.
      if (_parameters.size() == 3) {
        importHeight = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                  query, TID_INT64).getInt64();
        importWidth = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                                  query, TID_INT64).getInt64();
        if (importHeight <= 0 || importHeight > originalHeight) {
          throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
                               << "The import height is invalid.";
        }
        if (importWidth <= 0 || importWidth > originalWidth) {
          throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
                               << "The import width is invalid.";
        }
      }
      DimensionDesc i("i", 1, importHeight, importHeight, 0);
      DimensionDesc j("j", 1, importWidth, importWidth, 0);
      outputDims.push_back(i);
      outputDims.push_back(j);
    } 
    else {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANT_OPEN_FILE) << tiffName;
    }
    return ArrayDesc("LoadTIFF", outputAttributes, outputDims, defaultPartitioning());
  }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalLoadTIFF, "loadtiff");

} //namespace scidb
