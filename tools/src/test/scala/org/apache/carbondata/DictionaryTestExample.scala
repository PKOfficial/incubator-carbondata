package org.apache.carbondata

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.carbondata.core.util.CarbonProperties


object DictionaryTestExample extends App{

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath
  val storeLocation = s"$rootPath/examples/spark2/target/store"
  val warehouse = s"$rootPath/examples/spark2/target/warehouse"
  val metastoredb = s"$rootPath/examples/spark2/target"

  val carbonProperties = CarbonProperties.getInstance()

  import org.apache.spark.sql.CarbonSession._

  val ss = SparkSession
    .builder
    .master("local[1]")
    .appName("dictionary test example")
    .config("spark.sql.warehouse.dir", warehouse)
    .getOrCreateCarbonSession(storeLocation, metastoredb)

  ss.sparkContext.setLogLevel("ERROR")

  ss.sql(s"DROP TABLE IF EXISTS my_table")

  ss.sql(
    s"""CREATE TABLE my_table(name STRING, ocuupation STRING, salary INTEGER, age INTEGER, dob DATE) STORED BY 'carbondata'"""
      .stripMargin)

  val path = s"$rootPath/tools/src/main/resources/user.csv"

  // scalastyle:off
  ss.sql(
    s"""
       | LOAD DATA LOCAL INPATH '$path'
       | INTO TABLE my_table
       | options('FILEHEADER'='name,occupation,salary,age,dob')
       """.stripMargin)

  // scalastyle:on
  ss.sql("""
             SELECT *
             FROM my_table
              """).show

  // "name,occupation,salary,age,dob"
  ss.sql("DROP TABLE IF EXISTS carbon_table")

}
