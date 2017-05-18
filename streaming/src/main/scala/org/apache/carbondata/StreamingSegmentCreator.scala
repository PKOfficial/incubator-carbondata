package org.apache.carbondata

import org.apache.carbondata.core.util.path.CarbonTablePath

class StreamingSegmentCreator(storeLocation: String, tableInfo: TableInfo) {

  private val tablePath = storeLocation + "/" + tableInfo.dbName + "/" + tableInfo.tableName

  def createStreamingSegment: String = {
    val tablePath = new CarbonTablePath(storeLocation, tableInfo.dbName, tableInfo.tableName)
      .getStreamingSegmentDir("0", "0")
    println(tablePath)
    tablePath
  }

}
