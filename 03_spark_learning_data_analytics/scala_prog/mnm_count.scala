/* reading data in scala */

package main.scala.chapter2

import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.functions._


object MnMcount {


    def main(args: Array[String]) {
    val spark = SparkSession
                .builder
                .appName("MnMCount")
                .getOrCreate() 
                
                       
    
    /*if (args.length < 1 ) {


         print("Usage: m&m <file> ",sys.stderr())
         sys.exit(1)
    }*/
    
    /*val mnmFile = args(0) */ 


    // read file into a spark dataframe 


    val mnm = spark.read.format("csv") 
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/home/amogh/Documents/spark_certification/LearningSparkV2-master/databricks-datasets/learning-spark-v2/mnm_dataset.csv")
            
            
            
    
     spark.stop()
    }
}




