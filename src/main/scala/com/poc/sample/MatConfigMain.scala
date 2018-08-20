package com.poc.sample

import java.io.{BufferedWriter, File, FileWriter}

import com.poc.sample.Models.{CIAMaterialConfig, MaterialConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.json4s._
import org.json4s.native.Serialization.writePretty

import scala.io.Source

object MatConfigMain {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseFile(new File("config/application.conf"))
    val ciaMatConfig = CIAMaterialConfig(config.getString("conf.appName"), config.getString("conf.environment"), config.getString("conf.clusterName"), config.getString("conf.sourceName"), config.getString("conf.kerberosPrincipal"), config.getString("conf.kerberosKeyTabLocation"),
      config.getBoolean("conf.esStatusIndicator"), config.getString("conf.esIndex"), config.getString("conf.schemaDirectory"), config.getBoolean("conf.attunityCDCIndicator"), config.getString("conf.cdcJournalFields"), config.getBoolean("conf.createBaseTable"),
      config.getBoolean("conf.createBaseTableFromScooped"), config.getBoolean("conf.incrementalHiveTableExist"), config.getString("conf.seqColumn"), config.getString("conf.headerOperation"), config.getString("conf.deleteIndicator"), config.getString("conf.beforeImageIndicator"), config.getString("conf.mandatoryMetadata"),
      true, null)

    val matConfigMain = new MatConfigMain(config.getString("conf.tableSchemaKeyFile"))
    val materialConfigs = matConfigMain.readMaterializerConfig()
    val rejiggedMatConfigList: Seq[MaterialConfig] = materialConfigs.map(materialConfig => materialConfig.copy(createBaseTable = ciaMatConfig.createBaseTable, createBaseTableFromScooped = ciaMatConfig.createBaseTableFromScooped,
      incrementalHiveTableExist = ciaMatConfig.incrementalHiveTableExist, seqColumn = ciaMatConfig.seqColumn,
      headerOperation = ciaMatConfig.headerOperation, deleteIndicator = ciaMatConfig.deleteIndicator, beforeImageIndicator = ciaMatConfig.beforeImageIndicator,
      mandatoryMetaData = ciaMatConfig.mandatoryMetaData, pathToLoad = buildHDFSPath("to-be-processed", config, materialConfig),
      attunityUnpackedPath = buildHDFSPath("unpacked-path", config, materialConfig),
      attunityUnpackedArchive = buildHDFSPath("unpacked-archive", config, materialConfig),
      processedPathToMove = buildHDFSPath("processed", config, materialConfig)))
    val finalCIAConfig = ciaMatConfig.copy(materialConfigs = rejiggedMatConfigList.toList)
    implicit val formats = DefaultFormats
    val piedPierJSON = writePretty(finalCIAConfig)

    val file = new File(config.getString("conf.outputMaterializerConfig"))
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(piedPierJSON)
    bw.close()

    println("*****MATERIALIZATION CONFIG CREATED*****")
  }

  def buildHDFSPath(pathType: String, config: Config, materialConfig: MaterialConfig): String = {
    s"${config.getString("conf.rootPath")}/${config.getString("conf.sourceName")}/$pathType/${materialConfig.hiveDatabase}/${materialConfig.baseTableName}"
  }


}


class MatConfigMain(val fileName: String) extends MaterializerReader {

  override def readMaterializerConfig(): Seq[MaterialConfig] = {
    for {
      line <- Source.fromFile(fileName).getLines().toVector
      values = line.split(",").map(_.trim)
    } yield MaterialConfig(hiveDatabase = values(1), baseTableName = values(0), createBaseTable = false, createBaseTableFromScooped = false, incrementalHiveTableExist = false,
      incrementalTableName = s"${values(0)}_ext_incre_table", pathToLoad = null, attunityUnpackedPath = null, attunityUnpackedArchive = null, processedPathToMove = null,
      uniqueKeyList = values(2), partitionColumns = "", seqColumn = null, headerOperation = null, deleteIndicator = null,
      beforeImageIndicator = null, mandatoryMetaData = null)
  }
}