# The "Gamma" operator

*Last updated: April 7, 2018*

This repository contains almost all the code I wrote during my Ph.D. study at the University of Houston for the "Gamma" operator project.

The "Gamma" operator is a matrix operator that can be used to generate a summarization matrix (which we call the "Gamma" matrix) for a given input matrix. This "Gamma" matrix can be used as an intermediate matrix for computing many linear machine learning models including linear regression, PCA, Naive Bayes Classifier, K-means Clustering, etc.

This research has been published into several papers, listed below:

* **The Gamma Matrix to Summarize Dense and Sparse Data Sets for Big Data Analytics**
<br>Carlos Ordonez, **Yiqun Zhang**, Wellington Cabrera
<br>*IEEE Transactions on Knowledge and Data Engineering (TKDE)*，
<br>28(7): 1905-1918 (2016) [[IEEEXplore](http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=7439851)] [[PDF](http://www2.cs.uh.edu/~yzhang/research/w-2016-TKDE-gamma.pdf)]
* **A Cloud System for Machine Learning Exploiting a Parallel Array DBMS**
<br>**Yiqun Zhang**, Carlos Ordonez, Lennart Johnsson
<br>IEEE Proceedings of the DEXA Workshop 2017 [[PDF](http://www2.cs.uh.edu/~yzhang/research/gammacloud.pdf)]
* **The Gamma Operator for Big Data Summarization on an Array DBMS**
<br>Carlos Ordonez, **Yiqun Zhang**, Wellington Cabrera 
<br>*Journal of Machine Learning Research (JMLR): Workshop and Conference Proceedings* (BigMine 2014: 88-103) [[PDF](http://www2.cs.uh.edu/~yzhang/research/gamma.pdf)]
* **Time Complexity and Parallel Speedup to Compute the Gamma Summarization Matrix**
<br>Carlos Ordonez, **Yiqun Zhang**
<br>*Proc. Alberto Mendelzon International Workshop on Foundations of Data Management* (AMW), 2016 [[PDF](http://www2.cs.uh.edu/~yzhang/research/w-2016-AMW-gammathry.pdf)]
* **Big Data Analytics Integrating a Parallel Columnar DBMS and the R Language**
<br>**Yiqun Zhang**, Carlos Ordonez, Wellington Cabrera 
<br>*IEEE International Symposium on Cluster, Cloud and Grid Computing* (CCGrid), 2016 [[IEEEXplore](http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=7515749)] [[PDF](http://www2.cs.uh.edu/~yzhang/research/w-2016-CCGrid-colbda-short.pdf)]

### Repository structure
The earliest work of this project was published in the BigMine 2014 paper. The "Gamma" operator was initially written in C++, running on [SciDB](https://www.paradigm4.com/technology/#In-Database_Analytics). You can find those operators in the `gamma-scidb` directory, in different versions (dense/sparse), even with GPU acceleration (OpenACC). We later wanted to compare this SciDB implementation with Spark and Vertica, so there they are, the `gamma-spark` and the `gamma-vertica` directory. Also, there is a ScaLAPACK prototype authored by a previous student **Hadi Montakhabi** in the `scalapack-gamma` directory. We tried SciDB and Vertica for the K-means Clustering, but I believe the Vertica version was not done. In the `scidb-udos` folder, I included my customized SciDB operator for 2-D array loading as well as some other operators that I had fun with while learning how to write SciDB operators.

### Contact
I apologize for not having enough time to polish all that source code and to provide very detailed documentations. The code here is not so well engineered in my today's view, but it carries all my beautiful memories for my Ph.D. life. If you are interested in any of those work, please contact Dr. [Carlos Ordonez](http://www2.cs.uh.edu/~ordonez/index.html) via emails to carlos *at* central dot uh dot edu.
