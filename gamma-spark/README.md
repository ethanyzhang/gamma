I uploaded a `gamma-spark.patch` file, which contains some changes to the Spark source code I used to expose some APIs that I wanted to use in the Spark work. The head commit of the Spark repository when I applied this change is:

```
commit c481bdf512f09060c9b9f341a5ce9fce00427d08
Author: Davies Liu <davies@databricks.com>
Date:   Tue Feb 23 12:55:44 2016 -0800

    [SPARK-13329] [SQL] considering output for statistics of logical plan
    
    The current implementation of statistics of UnaryNode does not considering output (for example, Project may product much less columns than it's child), we should considering it to have a better guess.
    
    We usually only join with few columns from a parquet table, the size of projected plan could be much smaller than the original parquet files. Having a better guess of size help we choose between broadcast join or sort merge join.
    
    After this PR, I saw a few queries choose broadcast join other than sort merge join without turning spark.sql.autoBroadcastJoinThreshold for every query, ended up with about 6-8X improvements on end-to-end time.
    
    We use `defaultSize` of DataType to estimate the size of a column, currently For DecimalType/StringType/BinaryType and UDT, we are over-estimate too much (4096 Bytes), so this PR change them to some more reasonable values. Here are the new defaultSize for them:
    
    DecimalType:  8 or 16 bytes, based on the precision
    StringType:  20 bytes
    BinaryType: 100 bytes
    UDF: default size of SQL type
    
    These numbers are not perfect (hard to have a perfect number for them), but should be better than 4096.
    
    Author: Davies Liu <davies@databricks.com>
```
