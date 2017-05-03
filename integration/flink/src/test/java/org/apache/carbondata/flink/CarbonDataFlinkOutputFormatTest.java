package org.apache.carbondata.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class CarbonDataFlinkOutputFormatTest {

    String getRootPath() throws IOException {
        return new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Before
    public void beforeTest() throws Exception {
        CarbonDataFlinkOutputFormat.writeCount = 0;
    }

    @Test
    public void testOutputFormatWithProjection() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "Date", "country", "salary"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "salary"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumns(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb2")
                        .setTableName("testtable2")
                        .setRecordCount(recordCount);

        dataSource.output(outputFormat.finish());
        environment.execute();
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        assert (writeCount == recordCount);
    }

    @Test

    public void testOutputFormatWithProjection2() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();

        String[] columnTypes = {"Int", "Date", "String", "String", "String", "String", "Long"};
        String[] columnHeaders = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};

        CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormat =
                CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                        .setColumns(columnHeaders)
                        .setColumnTypes(columnTypes)
                        .setStorePath(getRootPath() + "/integration/flink/target/store")
                        .setDatabaseName("testdb")
                        .setTableName("testtable")
                        .setRecordCount(recordCount);

        dataSource.output(outputFormat.finish());
        env.execute();
        long writeCount = CarbonDataFlinkOutputFormat.getWriteCount();
        assert (writeCount == recordCount);
    }


    @Test
    public void testOutputFormatForWrongColumns() throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"SSN", "Address", "Contact_Number"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbonDataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        try {
            DataSet<Tuple2<Void, Object[]>> dataSource = environment.createInput(carbonDataFlinkInputFormat.getInputFormat());
            long recordCount = dataSource.count();

            String[] columnTypes = {"Int", "String", "Long"};
            String[] columnHeaders = {"SSN", "Address", "Contact_Number"};


            CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder outputFormatBuilder =
                    CarbonDataFlinkOutputFormat.buildCarbonDataOutputFormat()
                            .setColumns(columnHeaders)
                            .setColumnTypes(columnTypes)
                            .setStorePath(getRootPath() + "/integration/flink/target/store")
                            .setDatabaseName("testdb2")
                            .setTableName("testtable2")
                            .setRecordCount(recordCount);

            dataSource.output(outputFormatBuilder.finish());
            environment.execute();

        } catch (Exception e) {
            assert (true);
        }

    }
}
