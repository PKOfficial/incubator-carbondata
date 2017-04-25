package org.apache.carbondata.flink;

import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonProjection;

import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;

public class CarbonDataFlinkInputFormat {

  private String path;
  private String[] columns;
  private boolean isHdfsUrl;

  CarbonDataFlinkInputFormat(String path, String[] columns, boolean isHdfsUrl) {
    this.path = path;
    this.columns = columns;
    this.isHdfsUrl = isHdfsUrl;
  }

  boolean isValidPath() {
    return isHdfsUrl || (new File(path)).exists();
  }

  boolean isEmptyColumn() {

    return (columns.length == 0);
  }

  public HadoopInputFormat getInputFormat() {
    if (!isValidPath()) {
      throw new IllegalArgumentException("Invalid path to table.");
    } else if (isEmptyColumn()) {
      throw new IllegalArgumentException("Invalid columns for projection.");
    } else {
      CarbonProjection projections = new CarbonProjection();
      for (String column : columns) {
        projections.addColumn(column);
      }
      Configuration conf = new Configuration();

      CarbonInputFormat.setColumnProjection(conf, projections);
      try {
        return HadoopInputs
            .readHadoopFile(new CarbonInputFormat<Object[]>(), Void.class, Object[].class, path,
                new Job(conf));
      } catch (Exception e) {
        System.out.println("Could not create hadoop-input-format " + e);
        return null;
      }
    }

  }

}
