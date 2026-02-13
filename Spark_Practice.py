# Databricks notebook source
data = [("ravi", "bangalore"), ("akash", "delhi"), ("amar", "muzzafarpur"), ("avinash", ""  )]

schema = ["Name", "City"]

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.schema  # This can be used to impose schema while reading data

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.fillna("0")

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("*")

# COMMAND ----------

df2 = df.filter("Name == 'ravi'")
display(df2)

# COMMAND ----------

df.select(["Name", "City"]).display()

# COMMAND ----------

data = [("Mulakat", "Jarurai"), ("Jinda", "Kasam"), ("rorha", "aasma"), ("khoya", "pyaar")]

# COMMAND ----------

schema = ["desire", "result"]

# COMMAND ----------

df_love = spark.createDataFrame (data, schema)
df_love.display()

# COMMAND ----------

df_love.schema  #used for the imposing schema

# COMMAND ----------

df_love.columns

# COMMAND ----------

df_love.printSchema() 

# COMMAND ----------

df_love.dtypes

# COMMAND ----------

# create schmea forok the table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([StructField('name', StringType(),True ),
                    StructField('city', StringType(), True)]
                    )

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df_empty = spark.createDataFrame([],schema)
df_empty.display()  #while creating empty dataframe we have to provide the schmea and data into a sqaure bracket as empty i.e empty list

# COMMAND ----------

df2 = df.filter("name == 'khoya' and city =='pyaar' ") 
df2.display()

# COMMAND ----------

df.describe("*").show() # gives the statics about the columns 

# COMMAND ----------

from pyspark.sql.functions import lit

df.withColumn("Tumhe", lit('Dekhe')).display()  # using withColumn ("New_Column", lit(value that we want to pass))

# COMMAND ----------

df.withColumnRenamed("City", 'desire').display()

# COMMAND ----------

data = [
    (7369, "SMITH", "CLERK", 7902, "17-12-1980", 800, None, 20),
    (7499, "ALLEN", "SALESMAN", 7698, "20-02-1981", 1600, 300, 30),
    (7521, "WARD", "SALESMAN", 7698, "22-02-1981", 1250, 500, 30),
    (7566, "JONES", "MANAGER", 7839, "04-02-1981", 2975, None, 20),
    (7654, "MARTIN", "SALESMAN", 7698, "21-09-1981", 1250, 1400, 30),
    (7698, "SGR", "MANAGER", 7839, "05-01-1981", 2850, None, 30),
    (7782, "RAVI", "MANAGER", 7839, "06-09-1981", 2450, None, 10),
    (7788, "SCOTT", "ANALYST", 7566, "19-04-1987", 3000, None, 20),
    (7839, "KING", "PRESIDENT", None, "01-11-1981", 5000, None, 10),
    (7844, "TURNER", "SALESMAN", 7698, "09-08-1981", 1500, 0, 30),
    (7876, "ADAMS", "CLERK", 7788, "23-05-1987", 1100, None, 20),
    (7900, "JAMES", "CLERK", 7698, "12-03-1981", 950, None, 30),
    (7902, "FORD", "ANALYST", 7566, "12-03-1981", 3000, None, 20),
    (7934, "MILLER", "CLERK", 7782, "01-03-1982", 1300, None, 10),
    (1234, "SEKHAR", "doctor", 7777, None, 667, 78, 80),
    (7369, "SMITH", "CLERK", 7902, "17-12-1980", 800, None, 20),
    (7499, "ALLEN", "SALESMAN", 7698, "20-02-1981", 1600, 300, 30),
    (7521, "WARD", "SALESMAN", 7698, "22-02-1981", 1250, 500, 30),
    (7566, "JONES", "MANAGER", 7839, "04-02-1981", 2975, None, 20),
    (7654, "MARTIN", "SALESMAN", 7698, "21-09-1981", 1250, 1400, 30),
    (7698, "SGR", "MANAGER", 7839, "05-01-1981", 2850, None, 30),
    (7782, "RAVI", "MANAGER", 7839, "06-09-1981", 2450, None, 10),
    (7788, "SCOTT", "ANALYST", 7566, "19-04-1987", 3000, None, 20),
    (7839, "KING", "PRESIDENT", None, "01-11-1981", 5000, None, 10),
    (7844, "TURNER", "SALESMAN", 7698, "09-08-1981", 1500, 0, 30),
    (7876, "ADAMS", "CLERK", 7788, "23-05-1987", 1100, None, 20),
    (7900, "JAMES", "CLERK", 7698, "12-03-1981", 950, None, 30),
    (7902, "FORD", "ANALYST", 7566, "12-03-1981", 3000, None, 20),
    (7934, "MILLER", "CLERK", 7782, "01-03-1982", 1300, None, 10),
    (1234, "RAM", "CLERK", 7457, None, 494, 588, 80),
    (None, None, None, None, None, None, None, None),
    (None, None, None, None, None, None, None, None),
    (None, None, None, None, None, None, None, None)
]

# COMMAND ----------

# DBTITLE 1,Imposing_Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType,FloatType,DoubleType


schema = StructType([
    StructField('EMPNO', StringType(), True),
    StructField('ENAME', StringType(), True),
    StructField('JOB', StringType(), True),
    StructField('MGR', StringType(), True),
    StructField('HIREDATE', StringType(), True),
    StructField('SAL', StringType(), True),	
    StructField('COMM', StringType(), True),	
    StructField('DEPTNO', StringType(), True)
    ])

# COMMAND ----------

df_bd = spark.createDataFrame(data, schema=schema)

df_bd.display()

# COMMAND ----------

df_null = df_bd.fillna("0").display()

# COMMAND ----------

df_dna = df_bd.dropna().display()

# COMMAND ----------

df_bd.dropDuplicates(["EMPNO"]).display()

# COMMAND ----------

df_bd.distinct().display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.toDF("NAME", "CITY").display()

# COMMAND ----------

df.toDF("NAME", )

# COMMAND ----------

df_bd.display()

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import *

df_m_nv = df_bd.fillna({'COMM':0, 'SAL':0, 'JOB': 'TRAINEE', 'ENAME':'UNKNOWN', 'EMPNO': '1000', 'DEPTNO': '100', 'HIREDATE':'01-01-2026', 'MGR': 1000 })


# COMMAND ----------

df_m_nv.display()

# COMMAND ----------

df_bd.select(["COMM", "DEPTNO"]).display()

# COMMAND ----------

df_bd.toDF()

# COMMAND ----------

# toDF() function is used to convert the RDD(Resilience Distributed Dataset) into Dataframe.

# while doing so we have to pass the column header that is the schema to the RDD data because RDD doesn't have the schema information.

# COMMAND ----------

# DBTITLE 1,rdd
# rdd = spark.parallelize([(1, 'a'), (2, 'b'), (3, 'c')])

# rdd.display()

# COMMAND ----------

# DBTITLE 1,to_date()
# MAGIC %sql
# MAGIC
# MAGIC select to_date('01-01-2026', 'dd-MM-yyyy') 
# MAGIC -- #in to_date() function we have to give the format of the input date that we are passing and not the data format in which we want the output.

# COMMAND ----------

# DBTITLE 1,date_format()
# MAGIC %sql
# MAGIC select date_format('2026-01-01', 'yyyy-MM-dd') 
# MAGIC -- date_format(date, format): Returns a string representing the date formatted according to the specified format pattern.
# MAGIC -- The first argument is a date or timestamp, the second is the desired output format.

# COMMAND ----------

df_bd.display()

# COMMAND ----------

# DBTITLE 1,to_date()
df_hd = df_bd.withColumn("HIREDATE", to_date('HIREDATE', 'dd-MM-yyyy'))
df_hd.display()

# COMMAND ----------

df_hd.write.format("delta").mode("overwrite").saveAsTable("employee")

# COMMAND ----------

df_emp = spark.sql("select * from employee where JOB = 'CLERK' ")
df_emp.display()

# COMMAND ----------

df_emp.createOrReplaceTempView("emp_vw")

# COMMAND ----------

df_vw = spark.sql("select * from emp_vw ")
df_vw.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from employee where empno = 7369

# COMMAND ----------

# DBTITLE 1,get the table vesion
# MAGIC %sql
# MAGIC describe history employee

# COMMAND ----------

# DBTITLE 1,reading data version as of
# MAGIC %sql
# MAGIC
# MAGIC select * from employee version as of 1

# COMMAND ----------

df_bd.display()

# COMMAND ----------

df_bd = df_bd.fillna({"SAL":0, "COMM":0})
df_bd.display()

# COMMAND ----------

# DBTITLE 1,Cell 59
df_bd = (df_bd.withColumn("SAL", col("SAL").cast("int"))
             .withColumn("COMM", col("COMM").cast("int")))


# COMMAND ----------

df_bd = df_bd.withColumn("Total_Sal", col("SAL")+col("COMM") )

# COMMAND ----------

df_bd.display()

# COMMAND ----------

# DBTITLE 1,Slicing in DataFrame
df_bd.display()

# COMMAND ----------

df_bd = df_bd.drop("Total_Sal")

# COMMAND ----------

# DBTITLE 1,Slicing in DataFrame
df_bd = df_bd.withColumn("Total_Sal", df_bd.COMM+df_bd.SAL)
df_bd.display()

# COMMAND ----------

df_bd = df_bd.drop('Total_Sal')
df_bd = df_bd.withColumn("Total_Sal", df_bd["COMM"]+df_bd["SAL"])
df_bd.display()

# COMMAND ----------

# DBTITLE 1,function vs transforpmation
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_bd = df_bd.withColumn("CREATE_DATE", current_date() )

# df_bd = df_bd.withColumn("CREATE_DATE", col("CREATE_DATE").cast("date" ))
df_bd.display()

# Anything that have df.name() is a transformation and only name() then it is function via ion above example withColumn(0) is a transformation and current_date() is function 


# All transformations are availbale in dataframe API but all functions are available in a class called pyspark.sql.functions


# COMMAND ----------



df_bd = df_bd.withColumn("CREATE_DATE", current_date() ).withColumn("CREATED_BY", lit("ADB_JOB"))

df_bd.display()

# COMMAND ----------

# DBTITLE 1,Filter_Dataframe
df_filter = df_bd.filter(" DEPTNO = 10 AND  JOB = 'CLERK'   ")   #since the filter conditions are enclosed inside double quote they are behaves as SQL and hence they bare not case sensitive.
df_filter.display()

# COMMAND ----------

# DBTITLE 1,where
df_where = df_bd.where(" DEPTNO = 10 AND ENAME == 'Ravi' ")
 
df_where.display()

# COMMAND ----------

# DBTITLE 1,select the required column only
df_select = df_bd.select('EMPNO', 'ENAME', 'Total_Sal')  # in select() transformation we have to pass column name as a tuple
 
df_select.display()

# COMMAND ----------

df_select.write.format("delta").mode("overwrite").saveAsTable('emp_bd')

# by default the storage location of mangaed table is "user/hive/warehouse/emp_bd" 
# the data will be stored at this location "user/hive/warehouse/emp_bd"
                            # and
# metadata will be stored in the hive metastore                            

# COMMAND ----------

# DBTITLE 1,read by metastore
emp_bd = spark.sql(" select * from emp_bd " )
emp_bd.display()

# COMMAND ----------

# DBTITLE 1,read by path
# MAGIC %sql
# MAGIC
# MAGIC -- select * from delta.`/user/hive/warehouse/emp_bd/`
# MAGIC
# MAGIC -- Acute Accent (``)

# COMMAND ----------

# DBTITLE 1,create dataframe
df1 = spark.table("emp_bd")

df1.display()

# COMMAND ----------

df2 = spark.read.format("delta").load("/user/hive/warehouse/emp_bd")

# COMMAND ----------

# DBTITLE 1,Cell 76
df_country = spark.table("samples.wanderbricks.countries")
# df_country.display()  # Optional: display separately if needed

# COMMAND ----------

# DBTITLE 1,Cell 77
df_employee = spark.table("samples.wanderbricks.employees")
# df_employee.display()  # Optional: display separately if needed

# COMMAND ----------

# DBTITLE 1,joins
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_join = df_employee.join(df_country, df_employee["country"] == df_country["country"], "inner")
df_join.display()

# COMMAND ----------

# DBTITLE 1,left_semi vs inner
# left semi join and inner joins gives exactly same number of rows and except column. in left semi join we get column from only left table.

df_join_semi = df_employee.join(df_country, df_employee.country == df_country.country, "left_semi")

df_join_semi.display()

# COMMAND ----------

# DBTITLE 1,left_anti
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_join_anti = df_employee.alias("e").join(df_country, df_employee["country"] == df_country["country"], "left_anti")

df_join_anti.display()

# only columns from the left table 
# only those rows from left table that is not  common in right table



# COMMAND ----------

df_join.display()

# COMMAND ----------

df_gb = df_join.select("employee_id", "name", df_employee["country"], "continent")

df_gb.display()

# COMMAND ----------

# DBTITLE 1,groupby
from pyspark.sql.types import *

df_by = df_gb.groupBy("country").agg( count("employee_id").alias("Headcount") ).orderBy(col("Headcount").desc())

df_by.display()

# COMMAND ----------

# DBTITLE 1,orderBy
df_oby = df_gb.groupBy("country").agg( count("employee_id").alias("Headcount")  ).orderBy( "Headcount", ascending = False)

df_oby.display()

# COMMAND ----------

# DBTITLE 1,multiple agg
df_mag = df_gb.groupBy("continent", "country").agg( count("employee_id").alias("Headcount"), max("name").alias("Longest_Name")  ).orderBy(col("Headcount").desc(), col("Longest_Name").desc())
df_mag.display()

# COMMAND ----------

# DBTITLE 1,col()
# The col() function is available in the pyspark.sql.functions module.
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,StructType()
# Yes, StructType is available in pyspark.sql.types
from pyspark.sql.types import StructType

schema = StructType([])


# COMMAND ----------

# DBTITLE 1,window functions
df_bd.display()

# COMMAND ----------

# DBTITLE 1,dense_rank()
from pyspark.sql.window import Window

df_w = df_bd.withColumn( "sal_rnk", dense_rank().over( Window.partitionBy("DEPTNO").orderBy( col("SAL").desc() ) ))

df_w.select("DEPTNO", "SAL", "sal_rnk").dropna().display()

# COMMAND ----------

df_taxi = spark.table("samples.nyctaxi.trips")

df_taxi.display()

# COMMAND ----------

# DBTITLE 1,rank
df_try = spark.table("workspace.default.employee") 
 
from pyspark.sql.window import Window

df_group = df_try.dropna("all").groupby("DEPTNO", "JOB").agg( sum("SAL").alias("Total_Sal")).orderBy( col("DEPTNO").asc(), col("Total_Sal").desc())

df_rank = df_group.withColumn("rnk", rank().over(Window.orderBy( col("Total_Sal").desc() ) ))

df_rank.display()


# COMMAND ----------

# DBTITLE 1,row_number()
df_group_rn = df_try.dropna("all").groupBy("DEPTNO", "JOB").agg( sum("SAL").alias("Total_Sal")).orderBy( col("DEPTNO").asc(), col("Total_Sal").desc())

# df_row = df_group_rn.withColumn("rnk", row_number().over( Window.partitionBy("DEPTNO").orderBy( col("Total_Sal").desc() ) ) )

df_row = df_group_rn.withColumn("rnk", row_number().over( Window.orderBy( col("Total_Sal").desc() ) ) )

df_row.display()

# COMMAND ----------

# The default storage location for a managed table is: /user/warehouse/table/default/<table_name>

# /user/warehouse/table/default/employee

# COMMAND ----------

# DBTITLE 1,show create table
# MAGIC %sql
# MAGIC show create table employee

# COMMAND ----------

# DBTITLE 1,describe extended
# MAGIC %sql
# MAGIC -- give names of columns and it's data type
# MAGIC describe extended employee  

# COMMAND ----------

# DBTITLE 1,describe  detail
# MAGIC %sql
# MAGIC describe detail employee  -- give number of files in which table data is stored

# COMMAND ----------

# DBTITLE 1,describe history
# MAGIC %sql
# MAGIC describe history employee