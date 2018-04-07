# LoadTIFF SciDB Operator

This operator is used to import a TIFF image into a SciDB array.  
Currently this operator can only import grey scale image, but is should be easy to add supoort for RGB images.

###Usage:
    loadtiff('file name', [height], [width])
The *height* and *width* are optional parameters. If they are specified, the operator will only import a sub-range of the image according to the given height and width. Otherwise the operator will import the whole image.

The output array has two dimensions *(i, j)* representing the coordinates in the image, one attribute *val* stores the grey scale value (0-255).  
Please note that the chunk sizes of both dimensions are set to be same as their corresponding dimension size. So the output array will always have only one chunk.

###Prerequisites:
* The operator requires SciDB 15.7 or later.
* Please make sure *libtiff5* and *libtiff5-dev* are installed on your machine.  
  On Ubuntu you can install them with the following command:

        sudo apt-get install -y libtiff5 libtiff5-dev

* Environment variables ``SCIDB_SOURCE_PATH`` and ``SCIDB_VER`` need to be properly set.

###Compile and install:

    make && make install
Your SciDB server needs to be up when you run the install command.
