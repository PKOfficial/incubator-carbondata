package org.apache.streaming.session

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

object SessionCreator {

  def getSparkSession(): SparkSession = {
   SparkSession.builder
      .appName("StreamLocallyExample")
      .config("spark.master", "local")
      .getOrCreate()

  }

}
