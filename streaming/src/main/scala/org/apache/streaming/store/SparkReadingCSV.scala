package org.apache.streaming.store

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class SparkReadingCSV {

  def getCsvDataFrame(spark: SparkSession, tableDetails: TableDetails): DataFrame = {
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(getSchemaStructure(tableDetails.schema))
      .option("header","true")
      .csv(tableDetails.filePath)
    csvDF.printSchema()
    csvDF.map(data => data.getAs[String]("name"))
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
