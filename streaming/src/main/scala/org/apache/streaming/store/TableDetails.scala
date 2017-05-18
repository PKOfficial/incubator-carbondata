package org.apache.streaming.store

import org.apache.spark.sql.types.DataType

case class TableDetails(tableName: String, dbName: String, storePath: String, schema: Map[String, DataType],filePath:String)
