package org.apache.streaming.session

import org.apache.spark.sql.SparkSession

object SessionCreator {

  def getSparkSession(): SparkSession = {
    val spark: SparkSession = SparkSession.builder
      .appName("StreamLocallyExample")
      .config("spark.master", "local")
      .getOrCreate()
    spark
  }

}
