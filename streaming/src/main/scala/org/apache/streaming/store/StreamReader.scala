package org.apache.streaming.store

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.streaming.session.SessionCreator
import org.apache.streaming.socket.{JsonParser, SocketRead}


object StreamReader {
  def main(args: Array[String]) {
    /*val tableDetailForCsv = TableDetails("tableName",
      "databaseName",
      "/storepath",
      Map("name" -> StringType, "age" -> IntegerType),
      "./csv/my.csv")*/
    //val readCsv = new SparkReadingCSV
    val sparkSession: SparkSession = SessionCreator.getSparkSession()
    sparkSession.sparkContext.setLogLevel("ERROR")
    //val df: DataFrame = readCsv.getCsvDataFrame(sparkSession, tableDetailForCsv)
    val tableDetailForSocket = TableDetails("tableName",
      "databaseName",
      "/storepath",
      Map("value" -> StringType))
    val readSocketData = new SocketRead
    val socketDf: DataFrame = readSocketData.getSocketDataFrame(sparkSession)
    println("here------------I am")

    import sparkSession.implicits._
    /* val dataset = socketDf.map((x: Row) => x.getAs[String]("value"))
     val staticDf = sparkSession.sqlContext.read.json(dataset.rdd)
     staticDf.printSchema()
    socketDf.map { (x: Row) =>
      val df = sparkSession.sqlContext.read.json(x)
      df.show()
    }*/
    socketDf.as[String].map { (data: String) =>
      val parsedJsonString: Option[Any] = JsonParser.parseJson(data)
      if (parsedJsonString.isDefined) {
        val x: Unit = parsedJsonString.fold() { (data: Any) =>
          val y = data match {
            case list: List[Map[_,_]] => list
              .map((rowMap: Map[_, _]) => getDataFromMap(rowMap,
                tableDetailForSocket.schema)) //spark.sparkContext.parallelize(list)
            case colMap: Map[_,_] => getDataFromMap(colMap, tableDetailForSocket.schema)
          }
        }
      }
    }

    //    socketDf.as[String].map((x: String) => println("!!!" +x))
    //    sparkSession.streams.awaitAnyTermination()
    val query = socketDf.select("value").writeStream.outputMode("append").format("console").start()
    println("!!QUERY ID!!" + query.id)
    println("!!QUERY RUN ID!!" + query.runId)
    println("!!QUERY EXPLAIN!!" + query.explain)


    query.awaitTermination()
  }

  def getDataFromMap(rowMap: Any, tableSchema: Map[String, DataType]): List[(String, DataType)] = {
    val x: List[(String, DataType)] =(rowMap match {
      case rowData: Map[String, _] => rowData map { case (colName, colValue) =>
        val dataType: DataType = tableSchema.get(colName).get
        val y: Class[_ <: DataType] = dataType.getClass
        val value = colValue.asInstanceOf[DataType] /*parseValue(tableSchema.get(colName))*/
        (colName, value)
      }
    }).toList
    println("LIst for data>>>>>>>>>>>>>" + x)
    x
  }

  import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType}

  def parseValue(datatype: DataType): CarbonDataType = {
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
