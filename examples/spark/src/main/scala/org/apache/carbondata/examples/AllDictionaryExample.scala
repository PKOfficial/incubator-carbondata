/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils
import org.joda.time.{DateTime, DateTimeZone}

object AllDictionaryExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("AllDictionaryExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/100_UniqData.csv"
    //    val csvHeader = "ID,date,country,name,phonetype,serialname,salary"
    //    val dictCol = "|date|country|name|phonetype|serialname|"
    //    val allDictFile = ExampleUtils.currentPath + "/src/main/resources/data.dictionary"
    // extract all dictionary files from source data
    //    AllDictionaryUtil.extractDictionary(cc.sparkContext, testData, allDictFile, csvHeader, dictCol)
    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    cc.sql("DROP TABLE IF EXISTS uniqData")

    cc.sql(
      """
             CREATE TABLE uniqdata
             (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")
      """)

    cc.sql(
      s"""
              LOAD DATA INPATH '$testData' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')
           """)

    val t1 = System.currentTimeMillis()
    cc.sql(
      """
           SELECT * FROM uniqdata
      """).show()
    val t2 = System.currentTimeMillis()
    val timeTaken = t2 - t1
    println("Time taken : " + timeTaken)
    /*cc.sql("""
           SELECT * FROM t3 where floatField=3.5
           """).show()

    cc.sql("DROP TABLE IF EXISTS t3")*/

    // clean local dictionary files
    //    AllDictionaryUtil.cleanDictionary(allDictFile)
  }

}