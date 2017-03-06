package org.apache.carbondata.cardinality

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSourceProcessor {

  def getInputFromJson(sparkSession: SparkSession, filePath: String): Unit ={
    val data: DataFrame = sparkSession.read.json(filePath)
    println(data.schema.fields)
    println(data.printSchema())
    println(data.show())
  }

  def getInputFromParquet(sparkSession: SparkSession, filePath: String): Unit ={
    val data: DataFrame = sparkSession.read.parquet("")
  }

  def getInputFromOrc(sparkSession: SparkSession, filePath: String): Unit ={
    val data: DataFrame = sparkSession.read.jdbc("","","")
  }

  def getInputFromJdbc(sparkSession: SparkSession, filePath: String): Unit ={
    val data: DataFrame = sparkSession.read.orc("")
  }

  def getInputFromTable(sparkSession: SparkSession, tableName: String): Unit = {
    val data: DataFrame = sparkSession.read.table("abc")
  }

}

object DataSourceProcessor extends DataSourceProcessor
