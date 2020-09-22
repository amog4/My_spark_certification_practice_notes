from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

if __name__ == "__main__":

    #if len(sys.arv) != 2:

     #   print("Usage: m&m <file> ",sys.stderr())
      #  sys.exit(-1)



    # Build spark application 

    spark = SparkSession.builder.appName("PythonMnMcount").getOrCreate()

    #mnm_file = sys.argv[1]


    # read the file into dataframe using csv

    mnm_file = spark.read.format('csv').option('header',True).option('inferschema',True).\
        load('/home/amogh/Documents/spark_certification/LearningSparkV2-master/databricks-datasets/learning-spark-v2/mnm_dataset.csv')



    count_mnm_df = mnm_file.select("State","Color","Count").\
        groupBy("State","Color").\
            agg(count("Count").alias("Total")).\
            orderBy("Total",ascending=False)

    count_mnm_df.show(n=60, truncate=False)

    print("Total Rows = %d" % (count_mnm_df.count()))



    spark.stop()










