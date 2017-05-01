package org.apache.carbondata.flink;

import com.google.gson.Gson;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;
import org.apache.carbondata.processing.api.dataloader.SchemaInfo;
import org.apache.carbondata.processing.constants.TableOptionConstant;
import org.apache.carbondata.processing.csvload.BlockDetails;
import org.apache.carbondata.processing.csvload.CSVInputFormat;
import org.apache.carbondata.processing.csvload.CSVRecordReaderIterator;
import org.apache.carbondata.processing.csvload.StringArrayWritable;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.DataLoadExecutor;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

public class CarbondataStoreCreator {
    private static int count = 0;

    public DataType convertType(String type) {

        switch (type.toLowerCase()) {
            case "int":
                return DataType.INT;
            case "string":
                return DataType.STRING;
            case "double":
                return DataType.DOUBLE;
            case "float":
                return DataType.FLOAT;
            case "timestamp":
                return DataType.TIMESTAMP;
            case "decimal":
                return DataType.DECIMAL;
            case "date":
                return DataType.DATE;
            case "boolean":
                return DataType.BOOLEAN;
            case "long":
                return DataType.LONG;
            default:
                return DataType.NULL;
        }
    }

    public void createCarbonStore(AbsoluteTableIdentifier absoluteTableIdentifier, String columnString, String[] columnNames, String[] columnTypes, String sourcePath) {
        try {
            String factFilePath = sourcePath;
            File storeDir = new File(absoluteTableIdentifier.getStorePath());
            CarbonUtil.deleteFoldersAndFiles(storeDir);
            CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS,
                    absoluteTableIdentifier.getStorePath());

            CarbonTable table = createTable(absoluteTableIdentifier, columnNames, columnTypes);
            writeDictionary(factFilePath, table, absoluteTableIdentifier);
            CarbonDataLoadSchema schema = new CarbonDataLoadSchema(table);
            CarbonLoadModel loadModel = new CarbonLoadModel();
            String partitionId = "0";
            loadModel.setCarbonDataLoadSchema(schema);
            loadModel.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
            loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
            loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
            loadModel.setFactFilePath(factFilePath);
            loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());
            loadModel.setStorePath(absoluteTableIdentifier.getStorePath());
            loadModel.setDateFormat(null);
            loadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
                    CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
            loadModel.setDefaultDateFormat(CarbonProperties.getInstance().getProperty(
                    CarbonCommonConstants.CARBON_DATE_FORMAT,
                    CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));
            loadModel
                    .setSerializationNullFormat(
                            TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName() + "," + "\\N");
            loadModel
                    .setBadRecordsLoggerEnable(
                            TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName() + "," + "false");
            loadModel
                    .setBadRecordsAction(
                            TableOptionConstant.BAD_RECORDS_ACTION.getName() + "," + "FORCE");
            loadModel
                    .setIsEmptyDataBadRecord(
                            DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + "," + "false");
            loadModel.setCsvHeader(columnString);
            loadModel.setCsvHeaderColumns(loadModel.getCsvHeader().split(","));
            loadModel.setTaskNo("0");
            loadModel.setSegmentId("0");
            loadModel.setPartitionId("0");
            loadModel.setFactTimeStamp(System.currentTimeMillis());
            loadModel.setMaxColumns("10");
            executeGraph(loadModel, absoluteTableIdentifier.getStorePath());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private CarbonTable createTable(AbsoluteTableIdentifier absoluteTableIdentifier, String[] columnNames, String[] columnTypes) throws IOException {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setStorePath(absoluteTableIdentifier.getStorePath());
        tableInfo.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        ArrayList<Encoding> encodings = new ArrayList();
        encodings.add(Encoding.DICTIONARY);

        int x = 0;
        for (int i = 0; i < columnNames.length - 1; i++) {
            DataType type = convertType(columnTypes[i]);
            String colName = columnNames[i];
            ColumnSchema column = new ColumnSchema();
            column.setColumnName(colName);
            column.setColumnar(true);
            column.setDataType(type);
            column.setEncodingList(encodings);
            column.setColumnUniqueId(UUID.randomUUID().toString());
            column.setDimensionColumn(true);
            column.setColumnGroup(i + 1);
            columnSchemas.add(column);
            x = i + 1;
        }

        count++;
        DataType type = convertType(columnTypes[x]);
        String colName = columnNames[x];
        ColumnSchema salary = new ColumnSchema();
        salary.setColumnName(colName);
        salary.setColumnar(true);
        salary.setDataType(type);
        salary.setEncodingList(new ArrayList<Encoding>());
        salary.setColumnUniqueId(UUID.randomUUID().toString());
        salary.setDimensionColumn(false);
        salary.setColumnGroup(count);
        columnSchemas.add(salary);

        tableSchema.setListOfColumns(columnSchemas);
        SchemaEvolution schemaEvol = new SchemaEvolution();
        schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
        tableSchema.setSchemaEvalution(schemaEvol);
        tableSchema.setTableId(UUID.randomUUID().toString());
        tableInfo.setTableUniqueName(
                absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName() + "_"
                        + absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
        tableInfo.setLastUpdatedTime(System.currentTimeMillis());
        tableInfo.setFactTable(tableSchema);
        tableInfo.setAggregateTableList(new ArrayList<TableSchema>());

        CarbonTablePath carbonTablePath = CarbonStorePath
                .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
                        absoluteTableIdentifier.getCarbonTableIdentifier());
        String schemaFilePath = carbonTablePath.getSchemaFilePath();
        String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
        tableInfo.setMetaDataFilepath(schemaMetadataPath);
        CarbonMetadata.getInstance().loadTableMetadata(tableInfo);

        SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
        org.apache.carbondata.format.TableInfo thriftTableInfo = schemaConverter
                .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
                        tableInfo.getFactTable().getTableName());
        org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
                new org.apache.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
        thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
                .add(schemaEvolutionEntry);

        FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
        if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
            FileFactory.mkdirs(schemaMetadataPath, fileType);
        }

        ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
        thriftWriter.open();
        thriftWriter.write(thriftTableInfo);
        thriftWriter.close();
        return CarbonMetadata.getInstance().getCarbonTable(tableInfo.getTableUniqueName());
    }

    private void writeDictionary(String factFilePath, CarbonTable table, AbsoluteTableIdentifier absoluteTableIdentifier) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(factFilePath));
        String header = reader.readLine();
        String[] split = header.split(",");
        List<CarbonColumn> allCols = new ArrayList<CarbonColumn>();
        List<CarbonDimension> dims = table.getDimensionByTableName(table.getFactTableName());
        allCols.addAll(dims);
        List<CarbonMeasure> msrs = table.getMeasureByTableName(table.getFactTableName());
        allCols.addAll(msrs);

        Set<String>[] set = new HashSet[dims.size()];
        for (int i = 0; i < set.length; i++) {
            set[i] = new HashSet<String>();
        }
        String line = reader.readLine();
        while (line != null) {
            String[] data = line.split(",");
            for (int i = 0; i < set.length; i++) {
                set[i].add(data[i]);
            }
            line = reader.readLine();
        }

        Cache dictCache = CacheProvider.getInstance()
                .createCache(CacheType.REVERSE_DICTIONARY, absoluteTableIdentifier.getStorePath());
        for (int i = 0; i < set.length; i++) {
            ColumnIdentifier columnIdentifier = new ColumnIdentifier(dims.get(i).getColumnId(), null, null);
            CarbonDictionaryWriter writer =
                    new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath(),
                            absoluteTableIdentifier.getCarbonTableIdentifier(), columnIdentifier);
            for (String value : set[i]) {
                writer.write(value);
            }
            writer.close();
            writer.commit();
            org.apache.carbondata.core.cache.dictionary.Dictionary dict = (org.apache.carbondata.core.cache.dictionary.Dictionary) dictCache.get(
                    new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier(),
                            columnIdentifier, dims.get(i).getDataType()));
            CarbonDictionarySortInfoPreparator preparator =
                    new CarbonDictionarySortInfoPreparator();
            List<String> newDistinctValues = new ArrayList<String>();
            CarbonDictionarySortInfo dictionarySortInfo =
                    preparator.getDictionarySortInfo(newDistinctValues, dict, dims.get(i).getDataType());
            CarbonDictionarySortIndexWriter carbonDictionaryWriter =
                    new CarbonDictionarySortIndexWriterImpl(
                            absoluteTableIdentifier.getCarbonTableIdentifier(), columnIdentifier,
                            absoluteTableIdentifier.getStorePath());
            try {
                carbonDictionaryWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
                carbonDictionaryWriter.writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
            } finally {
                carbonDictionaryWriter.close();
            }
        }
        reader.close();
    }

    public void executeGraph(CarbonLoadModel loadModel, String storeLocation) throws Exception {
        new File(storeLocation).mkdirs();
        String outPutLoc = storeLocation + "/etl";
        String databaseName = loadModel.getDatabaseName();
        String tableName = loadModel.getTableName();
        String tempLocationKey = databaseName + '_' + tableName + "_1";
        CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
        CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
        CarbonProperties.getInstance().addProperty("send.signal.load", "false");
        CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true");
        CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1");
        CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true");
        CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true");
        CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
        CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000");
        CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false");
        CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000");

        String fileNamePrefix = "";

        String graphPath =
                outPutLoc + File.separator + loadModel.getDatabaseName() + File.separator + tableName
                        + File.separator + 0 + File.separator + 1 + File.separator + tableName + ".ktr";
        File path = new File(graphPath);
        if (path.exists()) {
            path.delete();
        }

        SchemaInfo info = new SchemaInfo();
        BlockDetails blockDetails = new BlockDetails(new Path(loadModel.getFactFilePath()),
                0, new File(loadModel.getFactFilePath()).length(), new String[]{"localhost"});
        Configuration configuration = new Configuration();
        CSVInputFormat.setCommentCharacter(configuration, loadModel.getCommentChar());
        CSVInputFormat.setCSVDelimiter(configuration, loadModel.getCsvDelimiter());
        CSVInputFormat.setEscapeCharacter(configuration, loadModel.getEscapeChar());
        CSVInputFormat.setHeaderExtractionEnabled(configuration, true);
        CSVInputFormat.setQuoteCharacter(configuration, loadModel.getQuoteChar());
        CSVInputFormat.setReadBufferSize(configuration, CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
                        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT));
        CSVInputFormat.setNumberOfColumns(configuration, String.valueOf(loadModel.getCsvHeaderColumns().length));
        CSVInputFormat.setMaxColumns(configuration, "10");

        TaskAttemptContextImpl hadoopAttemptContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID("", 1, TaskType.MAP, 0, 0));
        CSVInputFormat format = new CSVInputFormat();

        RecordReader<NullWritable, StringArrayWritable> recordReader =
                format.createRecordReader(blockDetails, hadoopAttemptContext);

        CSVRecordReaderIterator readerIterator = new CSVRecordReaderIterator(recordReader, blockDetails, hadoopAttemptContext);
        new DataLoadExecutor().execute(loadModel,
                storeLocation,
                new CarbonIterator[]{readerIterator});

        info.setDatabaseName(databaseName);
        info.setTableName(tableName);

        writeLoadMetadata(loadModel.getCarbonDataLoadSchema(), loadModel.getTableName(), loadModel.getTableName(),
                new ArrayList<LoadMetadataDetails>());

        String segLocation =
                storeLocation + "/" + databaseName + "/" + tableName + "/Fact/Part0/Segment_0";
        File file = new File(segLocation);
        File factFile = null;
        File[] folderList = file.listFiles();
        File folder = null;
        for (int i = 0; i < folderList.length; i++) {
            if (folderList[i].isDirectory()) {
                folder = folderList[i];
            }
        }
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (!files[i].isDirectory() && files[i].getName().startsWith("part")) {
                    factFile = files[i];
                    break;
                }
            }
            factFile.renameTo(new File(segLocation + "/" + factFile.getName()));
            CarbonUtil.deleteFoldersAndFiles(folder);
        }
    }

    public void writeLoadMetadata(CarbonDataLoadSchema schema, String databaseName,
                                         String tableName, List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setLoadEndTime(System.currentTimeMillis());
        loadMetadataDetails.setLoadStatus("SUCCESS");
        loadMetadataDetails.setLoadName(String.valueOf(0));
        loadMetadataDetails.setLoadStartTime(loadMetadataDetails.getTimeStamp(readCurrentTime()));
        listOfLoadFolderDetails.add(loadMetadataDetails);

        String dataLoadLocation = schema.getCarbonTable().getMetaDataFilepath() + File.separator
                + CarbonCommonConstants.LOADMETADATA_FILENAME;

        DataOutputStream dataOutputStream;
        Gson gsonObjectToWrite = new Gson();
        BufferedWriter brWriter = null;

        AtomicFileOperations writeOperation =
                new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));

        try {

            dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
            brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
                    Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

            String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray());
            brWriter.write(metadataInstance);
        } finally {
            try {
                if (null != brWriter) {
                    brWriter.flush();
                }
            } catch (Exception e) {
                throw e;

            }
            CarbonUtil.closeStreams(brWriter);

        }
        writeOperation.close();

    }

    public String readCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
        String date = null;
        date = sdf.format(new Date());
        return date;
    }


}