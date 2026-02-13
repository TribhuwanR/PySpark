# Databricks notebook source
# DBTITLE 1,check pointing
# MAGIC %md
# MAGIC
# MAGIC ### Quickly Repointing States with Checkpoint files
# MAGIC
# MAGIC
# MAGIC - databricks creates checkpointing after every 10 commits in delta table. it store the checkpointing file at the same folder where the delta_log is maintained.
# MAGIC -  checkpointing is used to maintain the state of the streaming
# MAGIC -  it helps in skipping the data while data analaysts read data from the delta tablesince checkpoinbting helps in identifying which file of delta tables should be opened, scanned AND CLOSED To get the required data.
# MAGIC # 
# MAGIC -  because to open nand close file happens ata disk and disk is slower than the RAM. so heigher the IOs the slower the process will be.
# MAGIC # 
# MAGIC -  checkpoint functions in the same indexing fuimnction in the SQL 
# MAGIC # 
# MAGIC - databricks maintainbs the checkpoint of only first 32 columnns of delta table. Hence the data moduller should make sure to keeps all those firstst where data analytics team have to apply the filter to filter the data from the delpota table.
# MAGIC # 
# MAGIC -  any filter applied on column that doesn't lies in the list of first 32 column of delta table will not get the advantage of the checkpointing
# MAGIC -  checkpoint files store data in delta file format
# MAGIC

# COMMAND ----------

# DBTITLE 1,%sql   optimize emp_data
# MAGIC %sql
# MAGIC optimize employee

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE Table_Name
# MAGIC
# MAGIC - optimize command is used to solve small file problem.
# MAGIC - the small files are deleted and large files of 1GB is created and later after compression the files size is reduced to 200MB to 300MB
# MAGIC - The small files problem is raised due to fooloowing reasons:
# MAGIC    - streaming loads that write data in samll batches
# MAGIC    - Hive-styled partioning that creates a separate file for each partition value
# MAGIC    - appending data into delta table in smalll amounts over time
# MAGIC - Having large number of small files will involve higher Disk IOs to open the small files, scan it and close it. 

# COMMAND ----------

# DBTITLE 1,create table  based on data
# MAGIC %sql
# MAGIC
# MAGIC -- create table emp_data location '/mnt/workshop/curated/employees'
# MAGIC

# COMMAND ----------

# DBTITLE 1,schema enforcement
# if we have table that have 5 columns and if we want to append data using dataframe having 7 columns the append will fails as we have impelemented the schema 

# COMMAND ----------

# DBTITLE 1,schema evolution
# adding new columns on existing table using mergeSchema option is callled schema evolution



# df_bd.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("employee_dept")

# COMMAND ----------

# DBTITLE 1,patitionBy()
# df_bd.write.format("delta").mode("overwrite").option("mergeSchema", "true"	).partitionBy("DEPTNO").saveAsTable("employee")

# use partitionBy() only when the size of data is greater than 1 TB as it eill create multiple partition based on DEPTNO and each partition will be stored on a separate folder and we can take adavantage of data skipping using checkpointing

# as partitionBy() will create lots of files that will involves higher Disk IOs and in turn it will reduce the performance.
# we have 8 vCores hence parallely we can run 8 files at a time. Hence to process 300 jobs we have to execeute in a batch of 300/8 = 37

# hence if we have small tables then we have to use clusterBy() instead of partitionBy()


# COMMAND ----------

# DBTITLE 1,ZORDER BY
# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE emp_data ZORDER BY (DayOfWeek)
# MAGIC
# MAGIC
# MAGIC -- while solving small files problem using OPTIMIZE command if we want to store the rows in same files that have same value of DayOfWeek then we have to use ZORDER BY command. 
# MAGIC
# MAGIC -- ZORDER BY  is used with OPTIMIZE command to store the rows in same files that have same value of DayOfWeek

# COMMAND ----------

# DBTITLE 1,liquid clustering
# to use the clusterBy() while creating the table only is called liquid clustering.
# This is also called logical partitioning 

%sql
create table emp_clust ("ENAME", string, "SAL", int, "DEPTNO", int)
using delta cluster By("DaysOfCover")

# This is used when the size of the table is less than 1 GB

# max to max we can only provide 4 columns inside clusterBy. and only those columns that are used by data  analytics team to filter data


 

# COMMAND ----------

# DBTITLE 1,repartition(4, "DEPTNO")
# repartition() is used to divide the complete data into into for parts and stores it into four different files.

# unlike the partitionBy() it doesn't create the folders and sub folders for each partitions

# if we only provide the repatition(4) then the partittion of data happens using ROUND_ROBIN criterion i.e. the complete data wioll be divided into four equal parts radanomly and stored.

#if we use repartition(4, "DEPTNO")  --> then the data will be divided using HASH PARTITION i.e. the data will be sorted on DEPTNO and similar value of DEPTNo will lies in the same file

df_emp_csv.repartition(4, "DEPTNO").write.mode("overwrite").format("delta").saveAsTable("emp1")

# repatition() has to done before saving the data into tables.

# the files are saved at loaction: "/user/hive/warehouse/emp1"

# this is called logical partitionong


# COMMAND ----------

# DBTITLE 1,to check pruning
# MAGIC %sql
# MAGIC
# MAGIC explain select * from emp1 where DEPTNO=10
# MAGIC
# MAGIC # this will have PartitionFilters
# MAGIC
# MAGIC --

# COMMAND ----------

# DBTITLE 1,DFP
To get the advantage of dynamic pruning following properties should be enabled:
    1. spark.databricks.otimizer.dynamicFilePruning 
    2. the delta table on the probe side of the join should be gretaer than 10GB size or more
       that can be obatined using:
           spark.conf.get("spark.databricks.optimizer.deltaTableSizeThreshold") this return 10,00000000 bytes to convert thios into GB
           10,00000000/1024/1024/1024 = 9.313 GB 

    3. Minimum nuber of file od deta table that required towards probe side is greater than 10 files
       that can be obatined using:
           spark.conf.get("spark.databricks.optimizer.dynamicFilePruningThreshold") 

    4. a. the inner table (the probe side) being joined should be in delta file format
       b. the join should be inner
       c. the join startegy should be broadcast join                             
# to get the advantage of data skipping we have to enable the following properties:
    spark.databricks.delta.optimizeWrite.enabled = true
    spark.databricks.delta.autoCompact.enabled = true

tom check the size of     


# COMMAND ----------

# DBTITLE 1,size of ile for better performnace
# if table size is less than 100GB then dive table size by 128 MB  
# and if table size is greater than 100GB then divide table size by 256 MB
#if table size is 700GB then by 512MB

# if table size is  1Tb or greater than 1TB the diveide tablesize/1024MB 

# COMMAND ----------

# DBTITLE 1,cloning
There are two types of cloning:
    1. shallow clone
    2. deep clone

# COMMAND ----------

# DBTITLE 1,shallow clone
# it is also known as zero copy clone or No copy clone or shallow clone

%sql
create or replace table emp_shallow shallow clone employee


# shallow clone is used to clone data from existing table without creating the actual file or with copying the data to newly created file.

# in shallow clone the new created table will store the version of delta table 

# to validate this, 
                  %fs ls /user/hive/warehouse/employee_shallow/

                  at this location we can only find the delta-log folder and no actual data


# shallow copy is used when the data analytics team asked for the data and later you want to compaire the current state of delta table with the that of shallow copy status.               

# COMMAND ----------

# DBTITLE 1,deep clone
# deep clone is similar to CTAS statement in SQL

%sql
create table emp_deep DEEP CLONE employee

# Here the new sets of file is created where we stores the copy of alaredy existing data 

# This is done when you think your opertaions may corrupt the existing data and you want to come back to the same state of the data 

# COMMAND ----------

# DBTITLE 1,CTAS
# CTAS is similar to deep clone

# but unlike the deep clone, CTAS doesn't copy the table properties, comments and constraints.

# any change in the base table will not reflect in the shallow clone table or deep clone table

%sql
create table if not exists emp_ctas as select * from employee where deptno = 10

# COMMAND ----------

# DBTITLE 1,%sql vacuum  employee
# vacuum command is used to delete the file from data lake that are no onger used by the delta table

# 

%sql
vacuum employee 



# if we want to dleete the data thta will raemain for the next 7 days i.e for the threshol-d period  then we have to 

%sql
vacuum employee retention o hours



# COMMAND ----------

# DBTITLE 1,vacuum command
# The VACUUM command in Databricks is used to permanently remove files that are no longer referenced by a Delta table.
# This helps reclaim storage space and maintain data lake hygiene.
# By default, VACUUM retains files for 7 days to ensure data recovery in case of failures or time travel queries.
# You can specify a custom retention period using the 'retention' option.

# Example:
# Removes files older than 7 days (default)
%sql
vacuum employee

# Removes files immediately (use with caution)
%sql
vacuum employee retention 0 hours  # to run thisok command we have to disable the default retention period for the vacuum table

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# DBTITLE 1,external table
# the table that has been created by providing the location for storage of data inside file is called external table

%sql
create table employee_external location '/user/hive/warehouse/employee_external'

# COMMAND ----------

# DBTITLE 1,compression
# Bzip2 is a compression algorithm that is commonly used for compressing files. It is a lossless compression algorithm, which means that the original file can be fully reconstructed from the compressed file.

# Bzip2 compression is followed beacause the compressed file supports parallel processing by spark and it has good compression ratio

# while transfering data from on-premise to cloud the file size is huge and since the lift and shift take place over the network and processing on network is slow as compare to RAM.

# so we compressed the file first before moving it to cloud. and then we can decompress the file in cloud and process it.

# compressed file are not processed in parallel. so it will be process through one job only.

# Compression should only be done to cold data example the data that is in landing layer.
# 
# The data in broze , silver and gold shouold not be compressed as it will increase the processing time beacuse we can't use the parallel processing and also we can't use the cache memory
# 
# 
#  




# COMMAND ----------

# DBTITLE 1,How to read data from multiple csv file
#provide everything path of the csv into the list as different element in load()

df = spark.read.format("csv").option("header", "true").load(["dbfs:/databricks-datasets/asa/airlines/2007.csv",  "dbfs:/databricks-datasets/asa/airlines/2008.csv"])


# df.cache()

df.count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,read mode
there are three types of read mode:
    1. permissive (default)
    2. dropmalford
    3. failfast

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/channels.csv", 
                """ CHANNEL_ID, CHANNEL_DSC, CHANNEL_CLASS, CHANNEL_CLASS_ID, CHANNEL_TOTAL, CHANNEL_TOTAL_ID
                3, Direct Sales, Direct, 12, Channel, total, 1
                9, Tele_Sales, Direct, 12, Channel, total, 1
                1, Partners, Others, 14, Channel total, 1, ravi, 2345, bangalore
                Sample, partners, others, ravi, Channel Total, 1, 10 Partners others 14 channelm total 1 """, True)

# COMMAND ----------

from pyspark.sql.types import *

df_schema = StructType( [StructField("CHANNEL_ID", IntegerType(), True),
                         StructField("CHANNEL_DSC", StringType(), True),
                         StructField("CHANNEL_CLASS", StringType(), True),
                         StructField("CHANNEL_CLASS_ID", IntegerType(), True),
                         StructField("CHANNEL_TOTAL", StringType(), True ])

# COMMAND ----------

# DBTITLE 1,PERMISSIVE
# PERMISSIVE read mode, in this read mode the databricks try to read all data from file and if the data of the file doesn't match with the schema column header and and its data type then iot give give the null value for that.


df = spark.read.option("mode", "PERMISSIVE").csv("/FIleStore/tables/channel.csv", header=True, schema = df_schema)
df.show()
# DROPMALFORMED read mode)

# COMMAND ----------

# DBTITLE 1,complex data type
# while dealing with json, avro, orc and parque file we can encounter the following complex data types:
1. ArrayTpe
2. StructType
3. MapType

# COMMAND ----------

# DBTITLE 1,ArrayType
# the dat is mention in [] bracket having mutipke values

# to flat-run the array data we have to use explode() commnad

# 


from pyspark.sql.functions import explode

df = spark.read.option("multiline", "true").json("dbfs:/FileStore/tables/sample.json")
df.display()


# df.show(truncate=False)
# df.select(explode("data")).show(truncate=False)
# # the data is mention in {} bracket having key value pair

# # to flat-run")





# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/samples/data/mllib/

# COMMAND ----------

