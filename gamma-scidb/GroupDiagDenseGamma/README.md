# GroupDiagDenseGamma SciDB Operator

This operator is used to compute the diagonal elements in the Gamma matrix for each target class in the input 2-D array.
The input array for this operator should be dense.

This Gamma operator is proposed in the paper:  
>**The Gamma Operator for Big Data Summarization on an Array DBMS** [[PDF](http://www2.cs.uh.edu/~yzhang/research/gamma.pdf)]  
Carlos Ordonez, Yiqun Zhang, Wellington Cabrera  
*Journal of Machine Learning Research (JMLR): Workshop and Conference Proceedings (BigMine 2014)*

Please cite the paper above if you need to use this code in your research work.

### Usage:
    GroupDiagDenseGamma(arrayName, numOfClasses, [idY])

**numOfClass:** The operator needs to know in advance the number of classes to determine the output array schema. The output array will give the diagonal elements of the Gamma matrix for each class sorted by the class ID in ascending order.

**idY*[optional]*:** The column ID of *Y*. By default *Y* is the last column in the array. However, you can designate any column as Y by giving its column ID **(start from 1)** in this parameter. The designated column will be move to the last before computing the diagonal elements of the Gamma matrix for each class.

### Prerequisites:
* The operator requires SciDB 15.7 or later.
* Environment variables ``SCIDB_SOURCE_PATH`` and ``SCIDB_VER`` need to be properly set.

### Compile and install:

    make && make install
Your SciDB server needs to be up when you run the install command.
