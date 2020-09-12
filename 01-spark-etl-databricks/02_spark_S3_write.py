
# --packages org.apache.hadoop:hadoop-aws:2.7.0
# import neecessary libraries
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName('s3')\
.getOrCreate()



spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

hadoop_conf=spark._jsc.hadoopConfiguration()
# see https://stackoverflow.com/questions/43454117/how-do-you-use-s3a-with-spark-2-1-0-on-aws-us-east-2
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.access.key", os.environ.get('access_key'))
hadoop_conf.set("fs.s3a.secret.key", os.environ.get('secret_key'))
hadoop_conf.set("fs.s3a.connection.maximum", "100000")
hadoop_conf.set("fs.s3a.endpoint", "s3." + "ap-south-1"+ ".amazonaws.com")






FlightData2015 = spark.\
                    read.option("inferSchema",'true').option("header",'true').\
                    csv('/home/amogh/Documents/spark certification/Spark-The-Definitive-Guide-master/data//flight-data/csv/2015-summary.csv')

    

FlightData2015.repartition(1).write.mode('overwrite').parquet('s3a://im-amogh/FlightData2015.parquet')



# For databricks following to be used 

"""

ACCESS_KEY = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
SECRET_KEY = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "<aws-bucket-name>"
MOUNT_NAME = "<mount-name>"

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

"""

spark.stop()