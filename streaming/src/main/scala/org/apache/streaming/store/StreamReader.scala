package org.apache.streaming.store

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.streaming.socket.{JsonParser, SocketRead}


object StreamReader {
  def main(args: Array[String]) {
    /*val tableDetailForCsv = TableDetails("tableName",
      "databaseName",
      "/storepath",
      Map("name" -> StringType, "age" -> IntegerType),
      "./csv/my.csv")*/
    //val readCsv = new SparkReadingCSV
    //    val sparkSession: SparkSession = SessionCreator.getSparkSession()

    val spark = SparkSession.builder
      .appName("StreamLocallyExample")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //val df: DataFrame = readCsv.getCsvDataFrame(sparkSession, tableDetailForCsv)
    val tableDetailForSocket = TableDetails("tableName",
      "databaseName",
      "/storepath",
      Map("value" -> StringType))
    val readSocketData = new SocketRead
    val socketDf: DataFrame = readSocketData.getSocketDataFrame(spark)
    println("here------------I am")
    socketDf.as[String].map { (data: String) =>
      val x: Option[Any] = JsonParser.parseJson(data)
      if (x.isDefined) {
        x.fold() { (data: Any) =>
          data match {
            case list: List[_] => list
              .map((rowMap: Any) => getDataFromMap(rowMap, tableDetailForSocket.schema)) //spark.sparkContext.parallelize(list)
            case _ => ""
          }
        }
      }
    }


    //    socketDf.as[String].map((x: String) => println("!!!" +x))
    //    sparkSession.streams.awaitAnyTermination()

    val query = socketDf.writeStream.outputMode("append").format("console").start()
      .awaitTermination()
  }

  def getDataFromMap(rowMap: Any, tableSchema : Map[String, DataType]): Unit = {
    rowMap match {
      case rowData: Map[String,_] => rowData map { case (colName, colValue) => parseValue(tableSchema.get(colName))
      }
    }
  }

  import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType}

  def parseValue(datatype : DataType): CarbonDataType = {
    datatype match {
      case IntegerType => CarbonDataType.INT
      case StringType => CarbonDataType.STRING
      case FloatType => CarbonDataType.FLOAT
      case ShortType => CarbonDataType.SHORT
      case DoubleType => CarbonDataType.DOUBLE
      case LongType => CarbonDataType.LONG
      case BooleanType => CarbonDataType.BOOLEAN
      case DecimalType() => CarbonDataType.DECIMAL
      case TimestampType => CarbonDataType.TIMESTAMP
      case DateType => CarbonDataType.DATE
    }
  }
}
