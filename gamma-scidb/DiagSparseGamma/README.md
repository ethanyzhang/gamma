# DiagSparseGamma SciDB Operator

This operator is used to compute the diagonal elements in the Gamma matrix for the input 2-D array.
The input array for this operator should be sparse.

This Gamma operator is proposed in the paper:  
>**The Gamma Operator for Big Data Summarization on an Array DBMS** [[PDF](http://www2.cs.uh.edu/~yzhang/research/gamma.pdf)]  
Carlos Ordonez, Yiqun Zhang, Wellington Cabrera  
*Journal of Machine Learning Research (JMLR): Workshop and Conference Proceedings (BigMine 2014)*

Please cite the paper above if you need to use this code in your research work.

### Usage:
    DiagSparseGamma(arrayName)

### Prerequisites:
* The operator requires SciDB 15.7 or later.
* Environment variables ``SCIDB_SOURCE_PATH`` and ``SCIDB_VER`` need to be properly set.

### Compile and install:

    make && make install
Your SciDB server needs to be up when you run the install command.
