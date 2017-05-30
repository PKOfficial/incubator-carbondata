//import java.io.{File, IOException}
//
//import org.apache.carbondata.core.constants.CarbonCommonConstants
//import org.apache.carbondata.core.util.CarbonProperties
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
//  /*@Test
//  @throws[Exception]
//  def getDataFromCarbonLocal() {
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
//    val columns = Array("ID", "Date", "country")
////        val path = "/integration/flink/target/store-input/testdb/testtable"
//    val path = "/integration/flink/src/test/resources/default/mytable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, false)
//    val dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val recordCount = dataSource.count
//
//    val columnTypes = Array("Int", "Date", "String")
//    val columnHeaders = Array("ID", "date", "country")
//    val dimensionColumns = Array("date", "country")
//    val outputFormat = CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat
//      .setColumnNames(columnHeaders)
//      .setColumnTypes(columnTypes)
//      .setStorePath("hdfs://localhost:54310/flink")
//      .setDatabaseName("default")
//      .setTableName("mytable")
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
//    val columns = Array("date", "country")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat("hdfs://localhost:54310/flink/default/mytable", columns, true)
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
//
//  // Reading from resource and writing to carbon on local
//  @Test
//  @throws[Exception]
//  def testOutputFormatWithProjectionString() {
//    val environment = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID","country")
//    //        val path = "/integration/flink/target/store-input/testdb/testtable"
//    val path = "/integration/flink/src/test/resources/default/mytable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, false)
//    val dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val recordCount = dataSource.count
//
//    val columnTypes = Array("Int","String")
//    val columnHeaders = Array("ID","country")
//    val dimensionColumns = Array("country")
//    val outputFormat = CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat
//      .setColumnNames(columnHeaders)
//      .setColumnTypes(columnTypes)
//      .setStorePath(getRootPath + "/integration/flink/target/store")
//      .setDatabaseName("default")
//      .setTableName("mytable")
//      .setRecordCount(recordCount)
//      .setDimensionColumns(dimensionColumns)
//    dataSource.output(outputFormat.finish)
//    environment.execute
//    val writeCount = CarbonDataFlinkOutputFormat.getWriteCount
//    assert(writeCount == recordCount)
//  }
//
//  // Reading from carbondata on local
//  @Test
//  @throws[Exception]
//  def getDataFromCarbonsLocalString() {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID", "country")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + "/integration/flink/target/store/default/mytable", columns, false)
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
//  // Reading from resource and writing to carbon on local
//  @Test
//  @throws[Exception]
//  def testOutputFormatWithProjectionDate() {
//    val environment = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID","date")
//    //        val path = "/integration/flink/target/store-input/testdb/testtable"
//    val path = "/integration/flink/src/test/resources/default/mytable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, false)
//    val dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val recordCount = dataSource.count
//    println(":::::::::::::::: record count  ::::::::::::::: " + recordCount)
//
//    val columnTypes = Array("Int","Date")
//    val columnHeaders = Array("ID","date")
//    val dimensionColumns = Array("date")
//    val outputFormat = CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat
//      .setColumnNames(columnHeaders)
//      .setColumnTypes(columnTypes)
//      .setStorePath(getRootPath + "/integration/flink/target/store")
//      .setDatabaseName("default")
//      .setTableName("mytableDate")
//      .setRecordCount(recordCount)
//      .setDimensionColumns(dimensionColumns)
//    dataSource.output(outputFormat.finish)
//    environment.execute
//    val writeCount = CarbonDataFlinkOutputFormat.getWriteCount
//    assert(writeCount == recordCount)
//  }
//
//  // Reading from carbondata on local
//  @Test
//  @throws[Exception]
//  def getDataFromFlinkLocalDate() {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID", "date")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + "/integration/flink/target/store/default/mytableDate", columns, false)
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
//  // Reading from spark example store on local using flink
//  @Test
//  @throws[Exception]
//  def getDataFromSparksLocalDate() {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID", "date","timestampField")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat("/home/sangeeta/projects/contribute/incubator-carbondata/examples/spark/target/store/default/flinktable", columns, false)
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
//  // Reading from resource and writing to carbon on hdfs
//  @Test
//  @throws[Exception]
//  def testOutputFormatWithProjectionDateAndTimeStamp() {
//    val environment = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID","date", "timestampField")
//    //        val path = "/integration/flink/target/store-input/testdb/testtable"
//    val path = "/examples/spark/target/store/default/flinktable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, false)
//    val dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val recordCount = dataSource.count
//    println(":::::::::::::::: record count  ::::::::::::::: " + recordCount)
//
//    val columnTypes = Array("Int","Date","Timestamp")
//    val columnHeaders = Array("ID","date","timestampField")
//    val dimensionColumns = Array("date", "timestampField")
//    val outputFormat = CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat
//      .setColumnNames(columnHeaders)
//      .setColumnTypes(columnTypes)
//      .setStorePath("hdfs://localhost:54310/flink")
//      .setDatabaseName("default")
//      .setTableName("mytableDateTimestamp")
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
//  def getDataFromCarbonsHDFSForDateTimestamp() {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("ID","date", "timestampField")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat("hdfs://localhost:54310/user/hive/warehouse/carbon.store/default/flinktable1", columns, true)
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
//  */
//
//  // Reading from resource and writing to carbon on hdfs
//  @Test
//  @throws[Exception]
//  def testOutputFormatWithProjectionDateAndTimeStamp() {
//    val environment = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("name", "decimalField")
//    //        val path = "/integration/flink/target/store-input/testdb/testtable"
//    val path = "/examples/spark/target/store/default/flinktable"
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath + path, columns, false)
//    val dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat)
//    val recordCount = dataSource.count
//
//    val columnTypes = Array("String", "Decimal(30,10)")
//    val columnHeaders = Array("name", "decimalField")
//    val dimensionColumns = Array("name")
//    val outputFormat = CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat
//      .setColumnNames(columnHeaders)
//      .setColumnTypes(columnTypes)
//      .setStorePath(getRootPath + "/integration/flink/target/store")
//      .setDatabaseName("default")
//      .setTableName("mytableDateTimestampDecimal")
//      .setRecordCount(recordCount)
//      .setDimensionColumns(dimensionColumns)
//    dataSource.output(outputFormat.finish)
//    environment.execute
//    val writeCount = CarbonDataFlinkOutputFormat.getWriteCount
//    assert(writeCount == recordCount)
//  }
//
//  @Test
//  @throws[Exception]
//  def getDataFromSparksLocalAllType() {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val columns = Array("name", "decimalField")
//    val carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat("/home/sangeeta/projects/contribute/incubator-carbondata/integration/flink/target/store/default/mytableDateTimestampDecimal", columns, false)
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
//
//}
