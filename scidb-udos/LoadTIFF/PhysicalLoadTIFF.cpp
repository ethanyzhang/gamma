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
 * @file PhysicalLoadTIFF.cpp
 * @author Yiqun Zhang <zhangyiqun9164@gmail.com>
 *
 * @brief Physical part of the LoadTIFF SciDB operator.
 */

#include <query/Operator.h>
#include <util/Network.h>
#include <tiffio.h>
using namespace std;

namespace scidb {

class PhysicalLoadTIFF : public PhysicalOperator
{
public:
  PhysicalLoadTIFF(string const& logicalName,
                         string const& physicalName,
                         Parameters const& parameters,
                         ArrayDesc const& schema):
      PhysicalOperator(logicalName, physicalName, parameters, schema)
  { }


  shared_ptr< Array> execute(vector< shared_ptr< Array>>& inputArrays, shared_ptr< Query> query)
  {
    shared_ptr< Array> outputArray(new MemArray(_schema, query));
    if (query->getInstanceID() != 0) {
      return outputArray;
    }
    string tiffName = ((shared_ptr< OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();
    TIFF* tif = TIFFOpen(tiffName.c_str(), "r");
    if (tif) {
      uint32 originalWidth, originalHeight;
      uint32 importWidth, importHeight;
      size_t scanline;
      tdata_t buf;
      Coordinates position(2, 1);
      shared_ptr< ArrayIterator> outputArrayIter = outputArray->getIterator(0);
      shared_ptr< ChunkIterator> outputChunkIter;
      scanline = TIFFScanlineSize(tif);
      TIFFGetField(tif, TIFFTAG_IMAGEWIDTH, &originalWidth);
      TIFFGetField(tif, TIFFTAG_IMAGELENGTH, &originalHeight);
      importWidth = originalWidth;
      importHeight = originalHeight;
      Value valueToWrite;
      if (_parameters.size() == 3) {
        importHeight = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                            query, TID_INT64).getInt64();
        importWidth = evaluate(((shared_ptr< OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                                            query, TID_INT64).getInt64();
      }
      buf = _TIFFmalloc(scanline);
      outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
      int64_t conv;
      for (uint32 row=1; row<=importHeight; row++) {
        position[0] = row;
        position[1] = 1;
        // row-1 because the output array has coordinate started from 1.
        TIFFReadScanline(tif, buf, row-1);
        unsigned char* data = reinterpret_cast<unsigned char*>(buf);
        // Only get gray scale value.
        for (uint32 col=0, j=1; col<scanline && j<=importWidth; col+=4, ++j) {
          outputChunkIter->setPosition(position);
          conv = data[col];
          valueToWrite.setInt64(conv);
          outputChunkIter->writeItem(valueToWrite);
          position[1]++;
        }
      }
      outputChunkIter->flush();
      _TIFFfree(buf);
      TIFFClose(tif);
    } 
    else {
      throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CANT_OPEN_FILE) << tiffName;
    }
    return outputArray;
  }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalLoadTIFF, "loadtiff", "PhysicalLoadTIFF");

} //namespace scidb
