package org.apache.carbondata.flink;

import org.apache.carbondata.flink.utils.UnzipUtility;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class CarbonDataFlinkOutputFormatTest {

    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatBenchmarkTest.class.getName());

    static String getRootPath() throws IOException {
        return new File(CarbonDataFlinkOutputFormatTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @BeforeClass
    public static void defineStore() throws IOException {
        String zipPath = getRootPath() + "/integration/flink/src/test/resources/store-input.zip";
        String zipDestinationPath = getRootPath() + "/integration/flink/target";

        UnzipUtility unzipUtility = new UnzipUtility();
        unzipUtility.unzip(zipPath, zipDestinationPath);
    }

    @Before
    public void beforeTest() throws Exception {
        CarbonDataFlinkOutputFormat.writeCount = 0;
    }

    @Test
    public void testOutputFormatWithProjection() throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};
        String[] dimensionColumns = {"date", "country"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb2")
                        .setTableName("testtable2")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        environment.execute();
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        assert (writeCount == recordCount);
    }

    @Test

    public void testOutputFormatForSelectAll() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String[] dimensionColumns = {"date", "country", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb")
                        .setTableName("testtable")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());
        env.execute();
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        assert (writeCount == recordCount);
    }


    @Test
    public void testOutputFormatForWrongColumns() throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"SSN", "Address", "Contact_Number"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbonDataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        try {
            DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbonDataFlinkInputFormat.getInputFormat());
            long recordCount = dataSource.count();

            String[] columnTypes = {"Int", "String", "Long"};
            String[] columnHeaders = {"SSN", "Address", "Contact_Number"};
            String[] dimensioncolumns = {"Address"};

            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormatBuilder =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount)
                            .setDimensionColumns(dimensioncolumns);

            dataSource.output(outputFormatBuilder.finish());
            environment.execute();
            assert false;
        } catch (Exception e) {
            assert (true);
        }

    }

    @Test
    public void testOutputFormatForInvalidColumn() throws Exception {
        LOGGER.info("testOutputFormatForInvalidColumn : For mismatched columnTypes and columnHeaders");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname"};
        String[] dimensioncolumns = {"date", "country", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb")
                        .setTableName("testtable")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensioncolumns);

        dataSource.output(outputFormat.finish());

        try {
            env.execute();
            assert false;
        } catch (JobExecutionException ex) {
            assert true;
        }
    }

    @Test
    public void testOutputFormatForInvalidDimension() throws Exception {
        LOGGER.info("testOutputFormatForInvalidDimension : String columns not included in dimension");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "String", "String", "String", "String", "String", "Long"};
        String[] columnNames = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String[] dimensionColumns = {"date", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnNames)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb")
                        .setTableName("testtable")
                        .setRecordCount(recordCount)
                        .setDimensionColumns(dimensionColumns);

        dataSource.output(outputFormat.finish());

        try {
            env.execute();
            assert false;
        } catch (JobExecutionException ex) {
            assert true;
        }
    }

    @Test
    public void testOutputFormatForInvalidDimensions() throws Exception {
        LOGGER.info("testOutputFormatForInvalidDimension : Dimension columns are more than table columns");

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};
        String[] dimensionColumns = {"date", "country", "name", "phonetype", "serialname"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount)
                            .setDimensionColumns(dimensionColumns);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (JobExecutionException ex) {
            LOGGER.info("Caught Exception : " + ex);
            assert true;
        }
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        assert (writeCount == recordCount);
    }

    @Test
    public void testOutputFormatWithoutDimensions() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumnNames(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb2")
                        .setTableName("testtable2")
                        .setRecordCount(recordCount);

        dataSource.output(outputFormat.finish());
        environment.execute();
        assert false;
        } catch (IllegalArgumentException ex) {
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutStorepath() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.info(ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutDatabaseName() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.info(ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutTableName() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.info(ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutRecordCount() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2");

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.info(ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutColumnNames() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());

        String[] columnTypes = {"Int", "Date", "String", "Long"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(1000);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.info(ex.getMessage());
            assert true;
        }
    }

    @Test
    public void testOutputFormatWithoutColumnTypes() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/target/store-input/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnHeaders = {"ID", "date", "country", "salary"};

        try {
            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumnNames(columnHeaders)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormat.finish());
            environment.execute();
            assert false;
        } catch (IllegalArgumentException ex) {
            LOGGER.info(ex.getMessage());
            assert true;
        }
    }

    @AfterClass
    public static void removeStore() throws IOException {
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
    }

}
