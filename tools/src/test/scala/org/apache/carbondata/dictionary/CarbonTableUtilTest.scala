package org.apache.carbondata.dictionary

import java.io.File
import java.util.logging.FileHandler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.carbondata.{EmployeeRecord, TestHelper}
import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable


class CarbonTableUtilTest extends FunSuite with MockitoSugar with CarbonTableUtil {

  val globalDictionaryUtil = GlobalDictionaryUtil

  import TestHelper.sparkSession.implicits._


  test("test dictionary metastore creation") {

    val employeeRecord = List(EmployeeRecord("sangeeta", 22),EmployeeRecord("geetika", 24),EmployeeRecord("pallavi", 26),EmployeeRecord("prabhat", 35),EmployeeRecord("sangeeta", 22),EmployeeRecord("geetika",22),EmployeeRecord("sangeeta",22),EmployeeRecord("prabhat", 35))
    val employeeDataframe = TestHelper.sparkSession.sparkContext.parallelize(employeeRecord).toDF("name", "age")
val nameDf = employeeDataframe.select("name")
    val ageDf = employeeDataframe.select("age")
    val carbonTable  = new CarbonTable()
val absoluteTableIdentifier = new AbsoluteTableIdentifier("./target/store/T1", new CarbonTableIdentifier("", "", "1"))
    val cardinalityMatrix = List(new CardinalityMatrix("name", 0.7, nameDf), new CardinalityMatrix("age", 0.5, ageDf, IntegerType))
    //when(globalDictionaryUtil.writeDictionary(carbonTable, cardinalityMatrix, absoluteTableIdentifier)) thenReturn()
    createDictionary(cardinalityMatrix, employeeDataframe)
    /*val schemaFile = new File("./target/store/T1/Metadata/schema")

    val dictFile = new File("./target/store/T1/Metadata/name.dict")
    assert(dictFile.exists() && schemaFile.exists())*/
  }

}
