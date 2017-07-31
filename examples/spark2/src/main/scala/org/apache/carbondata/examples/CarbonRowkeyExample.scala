package org.apache.carbondata.examples

import java.io.File

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.SparkSession

object CarbonRowkeyExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val testData = s"$rootPath/examples/spark2/src/main/resources/partition_data.csv"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonPartitionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    // none partition table
    spark.sql("DROP TABLE IF EXISTS t0")
    spark.sql(
      """
        | CREATE TABLE IF NOT EXISTS t0
        | (
        | vin String,
        | phonenumber Long,
        | country String,
        | area String
        | )
        | PARTITIONED BY (logdate Timestamp)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01','ROWKEY'='vin')
      """.stripMargin)

    spark.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t0
           """)
    spark.sql("SELECT * FROM t0")
  }
}
