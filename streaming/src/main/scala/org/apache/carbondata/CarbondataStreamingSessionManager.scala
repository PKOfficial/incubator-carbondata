package org.apache.carbondata

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.carbondata.core.util.CarbonProperties


object CarbondataStreamingSessionManager extends App {

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../").getCanonicalPath

  val sessionID = java.util.UUID.fromString("c40e9178-574f-4879-a48b-f375d2a106d9")
  val carbonProperties = CarbonProperties.getInstance()
  //  val storeLocation = carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION)
  val warehouse = s"$rootPath/streaming/target/warehouse"
  val metastoredb = s"$rootPath/streaming/target"
  val storeLocation = s"$rootPath/streaming/target/store"

  import org.apache.spark.sql.CarbonSession._

  val ss = SparkSession
    .builder
    .master("local[1]")
    .appName("spark session example")
    .config("spark.sql.warehouse.dir", warehouse)
    .getOrCreateCarbonSession(storeLocation, metastoredb)

  ss.sparkContext.setLogLevel("ERROR")

  ss.sql(s"DROP TABLE IF EXISTS my_table")

  ss.sql(
  s"""CREATE TABLE my_table(c1 STRING, C2 STRING, C3 INTEGER) STORED BY 'carbondata'"""
    .stripMargin)

  println(sessionID)

  import ss.implicits._

  val df = ss.sparkContext.parallelize(1 to 10, 2).map { x => ("a", "b", x) }
    .toDF("c1", "c2", "c3")

  val tableInfo = TableInfo("default", "my_table")

  val streamingSegmentCreator = new StreamingSegmentCreator(storeLocation, tableInfo)
  streamingSegmentCreator.createStreamingSegment

}

case class TableInfo(dbName: String, tableName: String)
