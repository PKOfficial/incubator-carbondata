package org.apache.carbondata.dictionary

import java.util.UUID

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.SchemaEvolution
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata,
CarbonTableIdentifier}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.SchemaEvolutionEntry


trait CarbonTableUtil {

  val globalDictionaryUtil: GlobalDictionaryUtil

  /**
   * This method creates dictionary for the input dataframe
   * @param cardinalityMatrix
   * @param dataFrame
   */
  def createDictionary(cardinalityMatrix: List[CardinalityMatrix],
      dataFrame: DataFrame): Unit = {
    val (carbonTable, absoluteTableIdentifier) = createCarbonTableMeta(cardinalityMatrix, dataFrame)
    globalDictionaryUtil.writeDictionary(carbonTable, cardinalityMatrix, absoluteTableIdentifier)
  }


  /**
   * This method creates the schema file for the input data file
   * @param cardinalityMatrix
   * @param dataFrame
   * @return
   */
  private def createCarbonTableMeta(cardinalityMatrix: List[CardinalityMatrix],
      dataFrame: DataFrame): (CarbonTable, AbsoluteTableIdentifier) = {
    val tableInfo: TableInfo = new TableInfo()
    val tableSchema: TableSchema = new TableSchema()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(
      "./target/store/T1",
      new CarbonTableIdentifier("", "", UUID.randomUUID().toString))
    val columnSchemas = getColumnSchemas(cardinalityMatrix)
    tableSchema.setListOfColumns(columnSchemas)
    val schemaEvol: SchemaEvolution = new SchemaEvolution()
    schemaEvol.setSchemaEvolutionEntryList(List())
    val (schemaMetadataPath, schemaFilePath) = setTableSchemaDetails(tableSchema,
      schemaEvol,
      tableInfo,
      absoluteTableIdentifier)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo,
        tableInfo.getDatabaseName,
        tableInfo.getFactTable.getTableName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime)
    thriftTableInfo.getFact_table.getSchema_evolution
      .getSchema_evolution_history.add(schemaEvolutionEntry)
    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    (CarbonMetadata.getInstance()
      .getCarbonTable(tableInfo.getTableUniqueName), absoluteTableIdentifier)
  }

  /**
   *
   * @param tableSchema
   * @param schemaEvol
   * @param tableInfo
   * @param absoluteTableIdentifier
   * @return
   */
  private def setTableSchemaDetails(tableSchema: TableSchema,
      schemaEvol: SchemaEvolution,
      tableInfo: TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): (String, String) = {
    val storePath = "./target/store/T1"
    tableInfo.setStorePath("./target/store/T1")
    tableInfo.setDatabaseName("")
    tableSchema.setTableName("")
    tableSchema.setSchemaEvalution(schemaEvol)
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableInfo.setTableUniqueName(
      absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName + "_" +
      absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo.setAggregateTableList(List.empty[TableSchema].asJava)

    val carbonTablePath = new CarbonTablePath(absoluteTableIdentifier.getCarbonTableIdentifier, storePath)



    val schemaFilePath = carbonTablePath.getSchemaFilePath


    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    (schemaMetadataPath, schemaFilePath)
  }

  /**
   *
   * @param cardinalityMatrix
   * @return
   */
  private def getColumnSchemas(cardinalityMatrix: List[CardinalityMatrix]): List[ColumnSchema] = {

    val encoding = List(Encoding.DICTIONARY).asJava
    var columnGroupId = -1
    cardinalityMatrix.map { element =>
      val columnSchema = new ColumnSchema()
      columnSchema.setColumnName(element.columnName)
      columnSchema.setColumnar(true)
      columnSchema.setDataType(parseDataType(element.dataType))
      columnSchema.setEncodingList(encoding)
      columnSchema.setColumnUniqueId(element.columnName)
      columnSchema
        .setDimensionColumn(globalDictionaryUtil.isDictionaryColumn(element.cardinality))
      // TODO: assign column group id to all columns
      columnGroupId += 1
      columnSchema.setColumnGroup(columnGroupId)
      columnSchema
    }
  }

  import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType}

  /**
   * This method returns the CarbonData datatype for corresponding Spark datatype
   * @param dataType
   * @return
   */
  private def parseDataType(dataType: DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.STRING
      case FloatType => CarbonDataType.FLOAT
      case IntegerType => CarbonDataType.INT
      case ByteType => CarbonDataType.SHORT
      case ShortType => CarbonDataType.SHORT
      case DoubleType => CarbonDataType.DOUBLE
      case LongType => CarbonDataType.LONG
      case BooleanType => CarbonDataType.BOOLEAN
      case DateType => CarbonDataType.DATE
      case DecimalType.USER_DEFAULT => CarbonDataType.DECIMAL
      case TimestampType => CarbonDataType.TIMESTAMP
      case _ => CarbonDataType.STRING
    }
  }

}

object CarbonTableUtil extends CarbonTableUtil {
  val globalDictionaryUtil = GlobalDictionaryUtil
}
