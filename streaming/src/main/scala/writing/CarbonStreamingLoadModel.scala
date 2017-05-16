package writing

class CarbonStreamingLoadModel(val databaseName: String, val tableName: String, val carbonDataFileName: String,
                               val partitionId: String, val segmentId: String,
                               val storePath: String, val sessionId: String, val factFilePath: String,
                               val factTimeStamp: String, val badRecordsLoggerEnable: String,
                               val badRecordsAction: String, val maxColumns: String, val isEmptyDataBadRecord: String
                              ) extends Serializable{



}
