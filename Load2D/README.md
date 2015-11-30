# Load2D SciDB Operator

This operator is used to import a binary file with double-precison floating-point numbers into a 2-D SciDB array.

###Usage:
    load2d('file name', n, d)
*n* stands for the count of rows and *d* stands for the count of columns.

The output array has two dimensions *(i,j)* representing the coordinates and one attribute *val* that stores the floating-point number.  
Currently this operator has some limitations:

* It doesn't support importing sparse arrays.
* It also doesn't support partially filled chunks.  
  If the operator reaches EOF without filling up the current chunk, it will start over importing from the beginning of the file.

###Prerequisites:
* The operator requires SciDB 15.7 or later.
* Environment variables ``SCIDB_SOURCE_PATH`` and ``SCIDB_VER`` need to be properly set.

###Compile and install:

    make && make install
Your SciDB server needs to be up when you run the install command.

###Debugging:
Compiling the operator with ``make debug`` will let the operator dump debug information to log files. Every instance will create one log file called ``load2d-instance-[instance id].log`` under your home directory on the local machine.