package org.apache.streaming.socket

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.streaming.store.TableDetails

/**
 * Created by knoldus on 18/5/17.
 */
class SocketRead {

  def getSocketDataFrame(spark: SparkSession): DataFrame = {
    val socketDF: DataFrame = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    socketDF.printSchema()
    socketDF
  }

}
