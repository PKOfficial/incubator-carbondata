package org.apache.carbondata.flink;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

public class CarbonDataFlinkOutputFormat extends RichOutputFormat<Tuple2<Void, Object[]>> {

    private String[] columnNames;
    private String[] columnTypes;
    private String storePath;
    private String databaseName;
    private String tableName;
    public static long writeCount = 0;
    private long recordCount;
    private ArrayList<Tuple2<Void, Object[]>> records = new ArrayList<>();

    private String getSourcePath() throws IOException {
        String path = new File(this.getClass().getResource("/").getPath() + "../../../..").getCanonicalPath();
        return path + "/integration/flink/target/flink-records/record.csv";
    }

    @Override
    public void configure(Configuration parameters) { }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException { }

    @Override
    public void writeRecord(Tuple2<Void, Object[]> record) throws IOException {
        records.add(record);
        writeCount++;

        if (writeCount == recordCount) {                       // recordCount+1 to include header
            AbsoluteTableIdentifier absoluteTableIdentifier = new AbsoluteTableIdentifier(storePath, new CarbonTableIdentifier(databaseName, tableName, UUID.randomUUID().toString()));

            String sourcePath = getSourcePath();
            File sourceFile = new File(sourcePath);
            sourceFile.getParentFile().mkdirs();
            sourceFile.createNewFile();

            BufferedWriter bufferedWriter = null;
            FileWriter fileWriter = null;
            String columnString = "";

            try {
                fileWriter = new FileWriter(sourcePath);
                bufferedWriter = new BufferedWriter(fileWriter);

                for (int iterator = 0; iterator < columnNames.length; iterator++) {
                    columnString += columnNames[iterator] + ",";
                }
                columnString = columnString.substring(0, columnString.length() - 1);
                bufferedWriter.write(columnString + "\n");

                for (Tuple2<Void, Object[]> element : records) {
                    String row = (element.toString().substring(7, element.toString().length() - 2)).replace(" ", "");
                    bufferedWriter.write(row + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (bufferedWriter != null) {
                        bufferedWriter.close();
                    }
                    if (fileWriter != null) {
                        fileWriter.close();
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            CarbondataStoreCreator carbondataStoreCreator = new CarbondataStoreCreator();
            carbondataStoreCreator.createCarbonStore(absoluteTableIdentifier, columnString, columnNames, columnTypes, sourcePath);

        }
    }

    public static long getWriteCount() {
        return writeCount;
    }

    @Override
    public void close() throws IOException { }

    public static CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder buildCarbonDataOutputFormat() {
        return new CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder();
    }

    public static class CarbonDataOutputFormatBuilder {

        private final CarbonDataFlinkOutputFormat format = new CarbonDataFlinkOutputFormat();

        public CarbonDataOutputFormatBuilder() {
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setColumns(String[] columns) {
            this.format.columnNames = columns;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setColumnTypes(String[] columnTypes) {
            this.format.columnTypes = columnTypes;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setStorePath(String storePath) {
            this.format.storePath = storePath;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setDatabaseName(String databaseName) {
            this.format.databaseName = databaseName;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setTableName(String tableName) {
            this.format.tableName = tableName;
            return this;
        }

        public CarbonDataFlinkOutputFormat.CarbonDataOutputFormatBuilder setRecordCount(long recordCount) {
            this.format.recordCount = recordCount;
            return this;
        }

        public CarbonDataFlinkOutputFormat finish() {
            return this.format;
        }
    }

}
