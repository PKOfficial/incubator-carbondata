package org.apache.carbondata

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.carbondata.dictionary.CarbonTableUtil


case class CommandLineArguments(inputPath: String, fileHeaders: Option[List[String]] = None, delimiter: String = ",", quoteCharacter: String = " \"",
                                badRecordAction: String = "IGNORE")

class DataFrameHandler {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def startProcess(args: Array[String]): Boolean = {
    try {
      processDataFrame(getArguments(args))
      true
    }
    catch {
      case _: Exception => false
    }
  }

  def getArguments(args: Array[String]): CommandLineArguments = {
    args.length match {
      case 1 => val inputPath: String = args(0)
        CommandLineArguments(inputPath)
      case 2 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        CommandLineArguments(inputPath, fileHeaders)
      case 3 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        val delimiter = args(2)
        CommandLineArguments(inputPath, fileHeaders, delimiter)
      case 4 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        val delimiter = args(2)
        val quoteChar = args(3)
        CommandLineArguments(inputPath, fileHeaders, delimiter, quoteChar)
      case 5 => val inputPath = args(0)
        val fileHeaders = Some(args(1).split(",").toList)
        val delimiter = args(2)
        val quoteChar = args(3)
        val badRecordAction = args(4)
        CommandLineArguments(inputPath, fileHeaders, delimiter, quoteChar, badRecordAction)
    }
  }

  def processDataFrame(parameters: CommandLineArguments): Unit = {
    val isHeaderExist = parameters.fileHeaders.isDefined
    val dataFrame = loadData(parameters.inputPath, isHeaderExist)
    val cardinalityProcessor = new CardinalityProcessor
    val cardinalityMatrix: Seq[CardinalityMatrix] = cardinalityProcessor.getCardinalityMatrix(dataFrame, parameters)
    println("Cardinality Matrix is : " + cardinalityMatrix)

    CarbonTableUtil.createDictionary(cardinalityMatrix.toList, dataFrame)
    println("Dictionary created successfully !!!!")
  }

  def loadData(filePath: String, isHeaderExist: Boolean): DataFrame = {
    LOGGER.info("Starting with the demo project")
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    val df: DataFrame = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
    df.printSchema()
    df
  }


}
