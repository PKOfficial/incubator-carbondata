package org.apache.carbondata

import org.apache.carbondata.cardinality.DataSourceProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class DataSourceProcessorTest extends FunSuite with DataSourceProcessor{

  val sparkSession: SparkSession = TestHelper.sparkSession

  test("tset json file"){
    getInputFromJson(sparkSession, "/home/sangeeta/input1.json")
  }

}
