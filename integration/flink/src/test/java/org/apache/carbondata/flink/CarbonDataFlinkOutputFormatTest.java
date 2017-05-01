package org.apache.carbondata.flink;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CarbonDataFlinkOutputFormatTest extends TestCase {

    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatBenchmarkTest.class.getName());

    String getRootPath() throws IOException {
        return new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Test public void testOutputFormat() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"ID", "date", "country", "name", "phonetype", "serialname", "salary"};
        String path = "/integration/flink/src/test/resources/store/testdb/testtable";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        long recordCount = dataSource.count();
        LOGGER.info("::::::::::::Record Count:::::::::::::::" + recordCount);

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
        assert(writeCount == recordCount);
    }
}
