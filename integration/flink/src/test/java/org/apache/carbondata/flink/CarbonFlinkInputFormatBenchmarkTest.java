package org.apache.carbondata.flink;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class CarbonFlinkInputFormatBenchmarkTest {

    private final static Logger LOGGER = Logger.getLogger(CarbonFlinkInputFormatBenchmarkTest.class.getName());

    String getRootPath() throws IOException {
        return new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Test
    public void getDataFromCarbon() throws Exception {
        long t1 = System.currentTimeMillis();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"CUST_ID", "CUST_NAME", "ACTIVE_EMUI_VERSION", "DOB", "DOJ", "BIGINT_COLUMN1", "BIGINT_COLUMN2", "DECIMAL_COLUMN1", "DECIMAL_COLUMN2", "Double_COLUMN1", "Double_COLUMN2", "INTEGER_COLUMN1"};
        String path = "/integration/flink/src/test/resources/store/default/uniqdata";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);
        DataSource<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        int rowCount = dataSource.collect().size();
        assert (rowCount == 100);
        long t2 = System.currentTimeMillis();
        long timeTaken = t2 - t1;
        LOGGER.info("Time taken : " + timeTaken);
        assert(timeTaken < 16000);
    }
}
