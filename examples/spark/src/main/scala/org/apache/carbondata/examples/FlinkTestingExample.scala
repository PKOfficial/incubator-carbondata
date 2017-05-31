package org.apache.carbondata.examples

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object FlinkTestingExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("FlinkExampleTest")
    val testData = "/home/sangeeta/projects/contribute/incubator-carbondata/integration/flink/src/test/resources/record1.csv"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    cc.sql("DROP TABLE IF EXISTS flinkTable")

    cc.sql(
      """
             CREATE TABLE IF NOT EXISTS flinkTable (name String, decimalField decimal(30,10)) STORED BY 'carbondata'
      """)

    cc.sql(
      s"""
              LOAD DATA LOCAL INPATH '$testData' into table flinkTable
           """)

    cc.sql(
      s"""
         select * from flinkTable
       """).show()
  }

}