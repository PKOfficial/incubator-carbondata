package org.apache.streaming.store

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.streaming.session.SessionCreator

object StreamReader {
  def main(args: Array[String]) {
    val tableDetail=TableDetails("","","",Map("name"->StringType, "age"-> IntegerType),"/home/knoldus/Documents/Carbon-Data-Streaming/incubator-carbondata/streaming/src/main/resources/csv/my.csv")
    val readCsv=new SparkReadingCSV
    val sparkSession: SparkSession =SessionCreator.getSparkSession()
    val df:DataFrame = readCsv.getCsvDataFrame(sparkSession,tableDetail)
  }
}
