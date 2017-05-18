package org.apache.streaming.local

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.streaming.store.TableDetails

class SparkReadingCSV {

  def getCsvDataFrame(spark: SparkSession, tableDetails: TableDetails): DataFrame = {
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(getSchemaStructure(tableDetails.schema))
      .option("header","true")
      .csv(tableDetails.filePath)
    csvDF.printSchema()
    csvDF
  }

  private def getSchemaStructure(schema: Map[String, DataType]): StructType = {
    val listSchema: List[StructField] =schema.map { case (colName, colType) => StructField(colName, colType,true)}.toList
    val structSchema: StructType =StructType(listSchema)
    structSchema
  }

 private def matchSchema(schema:DataFrame):Boolean={
   if(schema.schema==StructType(List(StructField("name",StringType,true),StructField("age",IntegerType,true))))
     true
   else
     false
 }
}
