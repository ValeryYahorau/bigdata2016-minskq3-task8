# bigdata2016-minskq3-task8


###Run
```
mvn clean install
```

```
/usr/hdp/current/spark2-client/bin/spark-submit --class com.epam.bigdata2016.minskq3.task8.SparkApp --master yarn ~/bigdata2016-minskq3-task8-1.1.1-jar-with-dependencies.jar 
<warehouse_directory> <logs_directory/file> <tags_file> <cities_files>
```

e.g. 

/usr/hdp/current/spark2-client/bin/spark-submit --class com.epam.bigdata2016.minskq3.task8.SparkApp --master yarn /opt/Projects/bigdata2016-minskq3-task8/target/bigdata2016-minskq3-task8-1.1.1-jar-with-dependencies.jar 
hdfs:///tmp/sparkhw1
hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in1.txt hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in2.txt hdfs://sandbox.hortonworks.com:8020/tmp/sparkhw1/in3.txt
