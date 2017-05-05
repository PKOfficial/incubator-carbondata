package org.apache.carbondata.flink;


import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.carbondata.flink.exceptions.HadoopFormatException;

import java.io.File;

public class CarbonDataFlinkInputFormat {

    private String path;
    private String[] columns;
    private boolean isHdfsUrl;

    /**
     *
     * @param path path of the carbon table
     * @param columns columns of the table
     * @param isHdfsUrl whether 'path' is hdfs path or local path
     */
    public CarbonDataFlinkInputFormat(String path, String[] columns, boolean isHdfsUrl) {
        this.path = path;
        this.columns = columns;
        this.isHdfsUrl = isHdfsUrl;
    }

    /**
     * checks whether the number of columns are specified or not
     * @return true if number of columns equals zero else false
     */
    boolean isEmptyColumn() {
        return (columns.length == 0);
    }

    /**
     * if the path is local then it checks whether the path exists or not
     * @return true for valid path else false (check validity only for local path)
     */
    boolean isValidPath() {
        if (isHdfsUrl) {
            return true;
        } else {
            return (new File(path)).exists();
        }
    }

    /**
     * checks whether path to the table and columns for projects are valid or not, if valid,
     * then it returns hadoop input format, else an exception is thrown.
     *
     * @return Hadoop input format configured as per carbon flink input format
     */
    public HadoopInputFormat<Void, Object[]> getInputFormat() throws HadoopFormatException {
        if (!isValidPath()) {
            throw new IllegalArgumentException("Invalid path to table." + path);
        } else if (isEmptyColumn()) {
            throw new IllegalArgumentException("Invalid columns for projection.");
        } else {
            CarbonProjection projections = new CarbonProjection();
            for (String column : columns)
                projections.addColumn(column);
            Configuration conf = new Configuration();

            CarbonInputFormat.setColumnProjection(conf, projections);
            try {
                HadoopInputFormat<Void, Object[]> format = HadoopInputs.readHadoopFile(new CarbonInputFormat(),
                Void.class, Object[].class, path,new Job(conf));
                return format;
            } catch (Exception e) {
                throw new HadoopFormatException("Could not create hadoop-input-format " + e);
            }
        }

    }
}
