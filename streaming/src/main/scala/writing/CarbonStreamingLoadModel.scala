package writing

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.mutate.SegmentUpdateDetails
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentUpdateStatusManager}
import org.apache.carbondata.processing.model.CarbonDataLoadSchema
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants

case class CarbonStreamingLoadModel(carbonTableIdentifier: CarbonTableIdentifier,
                                    partitionId: String, streamingSegmentId: String,
                                    storePath: String,
                                    factFilePath: String, factTimeStamp: String,
                                    badRecordsLoggerEnable: String,
                                    badRecordsAction: String, maxColumns: String,
                                    isEmptyDataBadRecord: String,
                                    carbonDataLoadSchema: CarbonDataLoadSchema,
                                    aggTables: Array[String],
                                    aggTableName: String,
                                    isDirectLoad: Boolean,
                                    loadMetadataDetails: List[LoadMetadataDetails],
                                    @transient segmentUpdateDetails: List[SegmentUpdateDetails],
                                    @transient segmentUpdateStatusManager: SegmentUpdateStatusManager
                                   ) extends Serializable {

  def this(carbonTableIdentifier: CarbonTableIdentifier, streamingSegmentId: String, maxColumns: String, carbonDataLoadSchema: CarbonDataLoadSchema,
           aggTables: Array[String], aggTableName: String, isDirectLoad: Boolean, loadMetadataDetails: List[LoadMetadataDetails],
           segmentUpdateDetails: List[SegmentUpdateDetails], segmentUpdateStatusManager: SegmentUpdateStatusManager) = {

    this(carbonTableIdentifier, "0", streamingSegmentId, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL, DataLoadProcessorConstants.FACT_FILE_PATH,
      DataLoadProcessorConstants.FACT_TIME_STAMP, DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE, DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION,
      maxColumns, DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD, carbonDataLoadSchema, aggTables, aggTableName, isDirectLoad,
      loadMetadataDetails, segmentUpdateDetails, segmentUpdateStatusManager)

  }


}
