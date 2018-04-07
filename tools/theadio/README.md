# Multi-threading & I/O test
In Oct. 2015 we wanted to find out if there is any difference in performance between various apporoaches to do multi-threading and I/O operation in C++. So I wrote this program.

###Usage
    threadio "parameter1=value1;parameter2=value2 ..."
Parameters are:

* **file**: The input file name.
* **nthread**: Number of threads.
* **thread**: Thread implementation.  Options are:
  * c++11  
  * posix
* **io**: I/O implementation. Options are:
  * posix
  * stdio
  * fstream
* **blocksize**: Block size, number in bytes.

The script ``run`` is an example of running the program, data files are not included in this repository.
