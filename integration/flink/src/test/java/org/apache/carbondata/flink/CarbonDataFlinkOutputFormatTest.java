package org.apache.carbondata.flink;

import junit.framework.TestCase;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class CarbonDataFlinkOutputFormatTest {

    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatBenchmarkTest.class.getName());

    String getRootPath() throws IOException {
        return new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Before
    public void setUpBeforeClass() throws Exception {
        CarbonDataFlinkOutputFormat.writeCount = 0;
    }

    @Test public void testOutputFormat() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String[] dimensioncolumns = {"date", "country", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                .setColumns(columnHeaders)
                .setColumnTypes(columnTypes)
                .setStorePath(getRootPath() + "/integration/flink/target/store")
                .setDatabaseName("testdb")
                .setTableName("testtable")
                .setRecordCount(recordCount)
                .setDimensionColumns(dimensioncolumns);

        dataSource.output(outputFormat.finish());
        env.execute();
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        assert(writeCount == recordCount);
    }


    @Test public void testOutputFormatForInvalidColumn() throws Exception {
        LOGGER.info("testOutputFormatForInvalidColumn : For mismatched columnTypes and columnHeaders");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname"};
        String[] dimensioncolumns = {"date", "country", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumns(columnHeaders)
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

    @Test public void testOutputFormatForInvalidDimension() throws Exception {
        LOGGER.info("testOutputFormatForInvalidDimension : String columns not included in dimension");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "String", "String", "String", "String", "String", "Long"};
        String[] columnNames = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String[] dimensioncolumns = {"date", "name", "phonetype", "serialname"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumns(columnNames)
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


}
