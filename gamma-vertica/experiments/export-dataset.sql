\a
\t
\f,
\o /home/vertica/dataset/kdd010kd010.csv
SELECT * FROM kdd010kd010;
\o /home/vertica/dataset/kdd010kd020.csv
SELECT * FROM kdd010kd020;
\o /home/vertica/dataset/kdd010kd040.csv
SELECT * FROM kdd010kd040;
\o /home/vertica/dataset/kdd010kd080.csv
SELECT * FROM kdd010kd080;
\o /home/vertica/dataset/kdd100kd010.csv
SELECT * FROM kdd100kd010;
\o /home/vertica/dataset/kdd100kd020.csv
SELECT * FROM kdd100kd020;
\o /home/vertica/dataset/kdd100kd040.csv
SELECT * FROM kdd100kd040;
\o /home/vertica/dataset/kdd100kd080.csv
SELECT * FROM kdd100kd080;
\o /home/vertica/dataset/kdd001md010.csv
SELECT * FROM kdd001md010;
\o /home/vertica/dataset/kdd001md020.csv
SELECT * FROM kdd001md020;
\o /home/vertica/dataset/kdd001md040.csv
SELECT * FROM kdd001md040;
\o /home/vertica/dataset/kdd001md080.csv
SELECT * FROM kdd001md080;

hdfs dfs -put /home/vertica/dataset/kdd010kd010.csv /dbdm2016/kdd010kd010.csv
hdfs dfs -put /home/vertica/dataset/kdd010kd020.csv /dbdm2016/kdd010kd020.csv
hdfs dfs -put /home/vertica/dataset/kdd010kd040.csv /dbdm2016/kdd010kd040.csv
hdfs dfs -put /home/vertica/dataset/kdd010kd080.csv /dbdm2016/kdd010kd080.csv
hdfs dfs -put /home/vertica/dataset/kdd100kd010.csv /dbdm2016/kdd100kd010.csv
hdfs dfs -put /home/vertica/dataset/kdd100kd020.csv /dbdm2016/kdd100kd020.csv
hdfs dfs -put /home/vertica/dataset/kdd100kd040.csv /dbdm2016/kdd100kd040.csv
hdfs dfs -put /home/vertica/dataset/kdd100kd080.csv /dbdm2016/kdd100kd080.csv
hdfs dfs -put /home/vertica/dataset/kdd001md010.csv /dbdm2016/kdd001md010.csv
hdfs dfs -put /home/vertica/dataset/kdd001md020.csv /dbdm2016/kdd001md020.csv
hdfs dfs -put /home/vertica/dataset/kdd001md040.csv /dbdm2016/kdd001md040.csv
hdfs dfs -put /home/vertica/dataset/kdd001md080.csv /dbdm2016/kdd001md080.csv