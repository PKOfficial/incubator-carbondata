//import java.io.{File, IOException}
//
//import org.apache.carbondata.flink.utils.UnzipUtility
//import org.apache.carbondata.flink.{CarbonDataFlinkInputFormat, CarbonDataFlinkOutputFormat}
//import org.apache.flink.api.java.ExecutionEnvironment
//import org.junit.{Before, Test}
//
//class FlinkOutputTest {
//
//  /*  @BeforeClass
//    @throws[IOException]
//    def defineStore() {
//      val zipPath = getRootPath + "/integration/flink/src/test/resources/store-input.zip"
//      val zipDestinationPath = getRootPath + "/integration/flink/target"
//      val unzipUtility = new UnzipUtility
//      unzipUtility.unzip(zipPath, zipDestinationPath)
//    }*/
//
//  @Before
//  @throws[Exception]
//  def beforeTest() {
//    CarbonDataFlinkOutputFormat.writeCount = 0
//  }
//
//  @throws[IOException]
//  private def getRootPath = new File(classOf[FlinkOutputTest].getResource("/").getPath + "../../../..").getCanonicalPath
//
//  // Input format reading from resources source
//  @Test
//  @throws[Exception]
//  def getDataFromCarbonLocal() {
//
//
//    val zipPath = getRootPath + "/integration/flink/src/test/resources/store-input.zip"
//    val zipDestinationPath = getRootPath + "/integration/flink/target"
//    val unzipUtility = new UnzipUtility
//    unzipUtility.unzip(zipPath, zipDestinationPath)
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID", "date", "country", "salary")
//    //    val path = "/integration/flink/target/store/testdb/testtable"
//    val path = "/integration/flink/src/test/resources/testdb/testtable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, true)
//    val dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val rowCount = dataSource.collect.size
//    val records = dataSource.collect
//    import scala.collection.JavaConversions._
//    for (record <- records) {
//      System.out.println(record.f1.mkString(","))
//    }
//    System.out.println("\n\nRecords Read: " + rowCount)
//    assert(rowCount == 10)
//  }
//
//  // Reading from resource and writing to carbon
//  @Test
//  @throws[Exception]
//  def testOutputFormatWithProjection() {
//    val environment = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID", "Date", "country", "salary")
//    //    val path = "/integration/flink/target/store-input/testdb/testtable"
//    val path = "/integration/flink/src/test/resources/testdb/testtable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, false)
//    val dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val recordCount = dataSource.count
//    val columnTypes = Array("Int", "String", "String", "Long")
//    val columnHeaders = Array("ID", "date", "country", "salary")
//    val dimensionColumns = Array("date", "country")
//    val outputFormat = CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat
//      .setColumnNames(columnHeaders)
//      .setColumnTypes(columnTypes)
//      .setStorePath("hdfs://localhost:54310/flink")
//      .setDatabaseName("default")
//      .setTableName("testtable2")
//      .setRecordCount(recordCount)
//      .setDimensionColumns(dimensionColumns)
//    dataSource.output(outputFormat.finish)
//    environment.execute
//    val writeCount = CarbonDataFlinkOutputFormat.getWriteCount
//    assert(writeCount == recordCount)
//  }
//
//  // Reading from carbondata on hdfs
//  @Test
//  @throws[Exception]
//  def getDataFromCarbonsHDFS() {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID", "date", "country", "salary")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat("hdfs://localhost:54310/flink/default/testtable2", columns, true)
//    val dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val rowCount = dataSource.collect.size
//    val records = dataSource.collect
//    import scala.collection.JavaConversions._
//    for (record <- records) {
//      System.out.println(record.f1.mkString(","))
//    }
//    System.out.println("\n\nRecords Read: " + rowCount)
//    assert(rowCount == 10)
//  }
//
//}
