package org.apache.carbondata

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext

object InferSchema extends App {

  val conf: SparkConf = new SparkConf().setMaster("local[1]")
    .setAppName("tool")
  val javaContext = new JavaSparkContext(conf)
  val sqlContext = new SQLContext(javaContext)

  val dataFrame = sqlContext.read.format("com.databricks.spark.csv").option("header", "false")
    .option("inferSchema", "true")
    .load("/home/kunal/Downloads/TestData/Data/uniqdata/2000_UniqData.csv")

  dataFrame.printSchema()

}
