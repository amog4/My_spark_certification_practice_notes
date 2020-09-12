"""
spark-submit --master local --jars /home/amogh/Documents/spark_certification/jar_files/mysql-connector-java-5.1.49.jar  04_jdbc_connect.py 
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local')\
    .appName('jdbc_connection').getOrCreate()


mysql_jdbc_host = 'localhost'
mysql_jdbc_port = 3307
mysql_jdbc_db = 'retail'
mysql_jdbc_user = 'retail_user'
mysql_jdbc_password = 'amogh'


query = " select * from orders "


my_sql  =  spark.read.format('jdbc').\
    option('url', 'jdbc:mysql://%s:%d/%s?useSSL=false&useUnicode=yes&characterEncoding=UTF-8&characterSetResults=UTF-8'%                 (mysql_jdbc_host,mysql_jdbc_port,mysql_jdbc_db))\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("user", mysql_jdbc_user) \
    .option("password", mysql_jdbc_password) \
    .option("query", query ) \
    .load()


#my_sql.show()


my_sql_parallelism = spark.read.format('jdbc').\
    option('url', 'jdbc:mysql://%s:%d/%s?useSSL=false&useUnicode=yes&characterEncoding=UTF-8&characterSetResults=UTF-8'%                 (mysql_jdbc_host,mysql_jdbc_port,mysql_jdbc_db))\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("user", mysql_jdbc_user) \
    .option("password", mysql_jdbc_password) \
    .option("dbtable", 'orders') \
    .option("partitionColumn",'order_id')\
    .option("lowerBound","1")\
    .option("upperBound","10000")\
    .option('numPartitions',"3")\
    .load()

my_sql_parallelism.explain()

spark.stop()