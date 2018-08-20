package com.poc.sample

object Models {

  case class CIAMaterialConfig(appName: String, environment: String, clusterName: String, sourceName: String, kerberosPrincipal: String, kerberosKeyTabLocation: String, esStatusIndicator: Boolean, esIndex: String, schemaDirectory: String, attunityCDCIndicator: Boolean, cdcJournalControlFields: String,
                               createBaseTable: Boolean, createBaseTableFromScooped: Boolean, incrementalHiveTableExist: Boolean, seqColumn: String, headerOperation: String, deleteIndicator: String,
                               beforeImageIndicator: String, mandatoryMetaData: String, overrideIndicator: Boolean, materialConfigs: List[MaterialConfig]
                              )

  case class MaterialConfig(hiveDatabase: String, baseTableName: String, createBaseTable: Boolean, createBaseTableFromScooped: Boolean, incrementalHiveTableExist: Boolean, incrementalTableName: String,
                            pathToLoad: String, attunityUnpackedPath: String, attunityUnpackedArchive: String,
                            processedPathToMove: String, uniqueKeyList: String, partitionColumns: String, seqColumn: String,
                            headerOperation: String, deleteIndicator: String, beforeImageIndicator: String, mandatoryMetaData: String)


}
