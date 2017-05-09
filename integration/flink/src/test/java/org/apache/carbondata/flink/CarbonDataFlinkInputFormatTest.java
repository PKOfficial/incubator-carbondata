package org.apache.carbondata.flink;

import org.apache.carbondata.flink.utils.UnzipUtility;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import scala.collection.TraversableOnce;

public class CarbonDataFlinkInputFormatTest {

    @BeforeClass
    public static void defineStore() throws IOException {
        System.out.println("Before class executing");
        String zipPath = getRootPath() + "/integration/flink/src/test/resources/store-input.zip";
        String zipDestinationPath = getRootPath() + "/integration/flink/target";

        UnzipUtility unzipUtility = new UnzipUtility();
        unzipUtility.unzip(zipPath, zipDestinationPath);

    }

    static String getRootPath() throws IOException {
        return new File(CarbonDataFlinkInputFormatTest.class.getResource("/").getPath() + "../../../..").getCanonicalPath();
    }

    @Test
    public void getDataFromCarbon() throws Exception {
        String zipPath = getRootPath() + "/integration/flink/src/test/resources/store-input.zip";
        String zipDestinationPath = getRootPath() + "/integration/flink/target";

        UnzipUtility unzipUtility = new UnzipUtility();
        unzipUtility.unzip(zipPath, zipDestinationPath);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] columns = {"id", "name"};
        String path = "/integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, true);

        DataSet<Tuple2<Void, Object[]>> dataSource = env.createInput(carbondataFlinkInputFormat.getInputFormat());
        int rowCount = dataSource.collect().size();
        System.out.println("\n\nRecords Read: " + rowCount);
        assert (rowCount == 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromInvalidPath() throws Exception {
        String[] columns = {"id", "name"};
        String path = "./flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        carbondataFlinkInputFormat.getInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataFromTableHavingInvalidColumns() throws Exception {
        String[] columns = {};
        String path = "integration/flink/target/store-input/default/t3";
        CarbonDataFlinkInputFormat carbondataFlinkInputFormat = new CarbonDataFlinkInputFormat(getRootPath() + path, columns, false);

        carbondataFlinkInputFormat.getInputFormat();
    }

    @AfterClass
    public static void removeStore() throws IOException {
        FileUtils.deleteDirectory(new File(getRootPath() + "/integration/flink/target/store"));
    }

}
