// Databricks notebook source
// MAGIC %md
// MAGIC Cdos Base notebook with commonn UDF for diffent scope-InscopePatient, PatientHicn, Empi, Location, Pod

// COMMAND ----------

// MAGIC %run ../../SnowparkConnection

// COMMAND ----------

// MAGIC %run ./CommonParameterWidgets

// COMMAND ----------

// MAGIC %run ./SparkSessionWrapper

// COMMAND ----------

// MAGIC %run ./CdosConstants

// COMMAND ----------

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import net.snowflake.client.jdbc.internal.joda.time.DateTime
import net.snowflake.spark.snowflake.Utils

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Calendar
import scala.util.control.Exception.ignoring
import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}
import scala.collection.JavaConverters._
import scala.language.{postfixOps, reflectiveCalls}
import com.snowflake.snowpark.{DataFrame=>SpDataFrame}
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.SaveMode.Overwrite
import com.snowflake.snowpark.types.{StringType, StructField, StructType}

class CdosBase(var notebookParams: scala.collection.mutable.Map[String,String], sfCDCM: Session) extends SparkSessionWrapper {


  var targetMarket: String = notebookParams("targetMarket")
  val targetMarketUC: String = targetMarket.toUpperCase()
  var databricksEnv: String = notebookParams("databricksEnv")
  var sfEnv: String = notebookParams("snowflakeEnv")
  var lookbackPeriod: String = notebookParams("lookbackPeriod")
  var snowflakeFormattedDate = ""
  var dbName: String = notebookParams("dbName")
  var fileDelimiter: String = "|"
  var fileExtension: String = ".txt"
  var azureStorageAccountName: String = ""
  var sasTokenAccount = ""
  var lastExtractDT = ""
  var sfWarehouse: String = notebookParams("sfWarehouse")
  var sfDatabase = ""
  var sfReferenceDatabase = ""
  var currentDT = ""
  var currentDate = ""
  var domainName = ""
  var sfSchema: String = notebookParams("sfSchema")
  val sfReferenceSchema = "OPTUMCARE_REFERENCE"
  val sfCDCMSchema = "CDCM"
  val sfCDMSchema = "CDM"
  val baseContainer: String = String.format("cdos-%s", targetMarket.toLowerCase)
  val landingContainer: String = baseContainer + "-" + "landing"
  val archiveContainer: String = baseContainer + "-" + "archive"
  var mainContainer = ""
  var tokenForArchiveContainer = ""
  var tokenForLandingContainer = ""
  val tempfileTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmssSS")).toString
  var consumer = ""
  var tempFilePrefix: String = ""
  var filePrefix: String = ""
  var Record_Start_Date = ""
  var path_suffix: String = notebookParams("dbName")
  var prefix: String = ""
  var landingMnt: String = ""
  var baseMnt: String = ""
  var extract_files: Seq[String] = null
  val fileLocation = notebookParams("fileLocation")
  var filterationTablefile_location = fileLocation+"filtration_xwalk.csv"
  var defaultTablefile_location = fileLocation+"default_xwalk.csv"
  var filename_location = fileLocation+"filename_xwalk.txt"
  val sastoken: String = "-sastoken"
  var sfOptions = scala.collection.mutable.Map("sfDatabase" -> "DUMMY") // This will be over-written in the method, setSnowflakeparameters
  var sfUrl = ""
  var sfUser = ""
  var sfPPK = ""
  val spSuffix: String = ""
  var query_directory = ""
  var fileName = ""
  var snowparkFileName = ""
  Record_Start_Date = notebookParams("Record_Start_Date")
  println(s"Record_Start_Date $Record_Start_Date")
  if (dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
    consumer = ""
  } else {
    consumer = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase()
  }
  /*
  * Updates the last_extract_info table with the previous extractDT for the given market and domain.
  *
  */
  def setNbParameters() = {
    // Get the parameter values.
    if (databricksEnv == "PRD") {
      azureStorageAccountName = "caredataextractssit"
      sasTokenAccount = "ocdp-".concat(databricksEnv).toLowerCase()
    }
    else {
      azureStorageAccountName = "caredataextracts"
      sasTokenAccount = "ocdp-".concat(databricksEnv).toLowerCase()
    }

    sfUrl = "snowflake".concat(sfEnv.toLowerCase.capitalize).concat("-sfUrl")
    println(sfUrl)
    sfUser = "snowflake".concat(sfEnv.toLowerCase.capitalize).concat("-sfUser")
    println(sfUser)
    sfPPK = "snowflake".concat(sfEnv.toLowerCase.capitalize).concat("-pem-private-key")
    println(sfPPK)

    sfDatabase = "OCDP_".concat(sfEnv).concat("_CDCM_DB")
    sfReferenceDatabase = "OCDP_".concat(sfEnv).concat("_OPTUMCARE_REFERENCE_DB")

    if (lookbackPeriod == "39") {
      lastExtractDT = LocalDate.now().plusMonths(-39).format(DateTimeFormatter.ofPattern("yyyy-MM")).concat("-01").concat(" 00:00:00.000")
    }
    if (databricksEnv.toLowerCase() == "prd") {
      if (dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
        tempFilePrefix = "TEMP_" + tempfileTimestamp + databricksEnv + "_"
        filePrefix = ""
      }
      else if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
        tempFilePrefix = "TEMP_" + tempfileTimestamp + databricksEnv + "_" + dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase() + "_"
        filePrefix = ""
      }
    }
    else if (databricksEnv.toLowerCase() == "dev" || databricksEnv.toLowerCase() == "stg") {
      tempFilePrefix = "TEMP_" + tempfileTimestamp + databricksEnv + "_"
      filePrefix = databricksEnv + "_"
    }
  }

 mainContainer = "cdos" + databricksEnv.toLowerCase()

  if (databricksEnv == "PRD")

  {

    mainContainer = "cdossit"

  }
  if (sfEnv == "SIT")
  {
    mainContainer = "cdosstg"
  }

  println(s"tempFilePrefix is: $tempFilePrefix")
  println(s"filePrefix is: $filePrefix")

  def setSnowflakeparameters() =
    ignoring(classOf[java.lang.ExceptionInInitializerError]) {
      // dbutils throws errors running locally outside of the cluster- swallow these
      try {
        val scope = "ocdp-".concat(databricksEnv).toLowerCase()
         sfOptions = scala.collection.mutable.Map(
          "sfUrl" -> dbutils.secrets.get(scope, sfUrl),
          "sfUser" -> dbutils.secrets.get(scope, sfUser),
          "pem_private_key" -> dbutils.secrets.get(scope, sfPPK),
          "sfDatabase" -> "OCDP_".concat(sfEnv).concat("_CDCM_DB"), // database
          // NOTE:
          // Removed "sfSchema" as this is different for each domain. This needs to be set explicitly before connecting with snowflake.
          "sfWarehouse" -> sfWarehouse
        )
        sfOptions
      } catch {
        case e: Throwable => {
          val sfOptionsFallback = scala.collection.mutable.Map(
            "sfUrl" -> sfUrl,
            "sfUser" -> sfUser,
            "pem_private_key" -> sfPPK,
            "sfDatabase" -> "OCDP_".concat(sfEnv).concat("_CDCM_DB"),
            "sfWarehouse" -> sfWarehouse
          )
          sfOptionsFallback
        }
      }
    }
  def setNbParametersWithSecrets() =
    ignoring(classOf[java.lang.ExceptionInInitializerError]) {
      // dbutils throws errors running locally outside of the cluster- swallow these
      try {
        tokenForArchiveContainer =
          dbutils.secrets.get(sasTokenAccount, archiveContainer + sastoken)
        tokenForLandingContainer =
          dbutils.secrets.get(sasTokenAccount, landingContainer + sastoken)
      } catch {
        case _: Throwable =>
      }
    }

  setNbParameters()
  setNbParametersWithSecrets()
  setSnowflakeparameters()

  println(s"tempFilePrefix is: $tempFilePrefix")
  println(s"filePrefix is: $filePrefix")

  val List_cdos = List("CAREDATA_EXTRACT_ELT.cdos", "CAREDATA_EXTRACT_ELT.cdos_uat", "CAREDATA_EXTRACT_ELT.cdos_arcadia", "CAREDATA_EXTRACT_ELT.cdos_arcadia_uat", "CAREDATA_EXTRACT_ELT.cdos_wellmed", "CAREDATA_EXTRACT_ELT.cdos_wellmed_uat")

  val list_used = if (databricksEnv.toLowerCase() == "dev" || databricksEnv.toLowerCase() == "stg") {
    List_cdos
  }
  else {
    List_cdos.filter(!_.contains("uat"))
  }

  val list_a = list_used.filter(_.contains("arcadia"))
  val list_w = list_used.filter(_.contains("wellmed"))



  def setCurrentDT() = {
    currentDT = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).toString()
    println("currentDT:" + currentDT)
  }

  def setCurrentDate() = {
    currentDate = new SimpleDateFormat("yyMMdd").format(Calendar.getInstance().getTime())
    print(currentDate)
  }

  def setLastExtractDT() = {
    val sqlQuery = s"""SELECT LAST_EXTRACT_DT as lastExtractDT FROM """ + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") + """ where FILE_NAME = '""" + domainName + """'"""
    println(sqlQuery)
    val df = sfCDCM.sql(sqlQuery)
    lastExtractDT = df.select("lastExtractDT").collect().map(_.getTimestamp(0)).mkString("")
    println("lastExtractDT:" + lastExtractDT)
  }


  def getLastExtractDate(): String = {
    var lastExtarctDate = lastExtractDT.take(10).replace("-", "").trim().substring(2)
    print(lastExtarctDate)
    return lastExtarctDate
  }

  def deleteFiles(srcMount: String, reportNamePrefix: String) = {
    try {
      println(reportNamePrefix)
      println(" Source : " + srcMount)
      extract_files = dbutils.fs.ls(srcMount).map(_.name).filter(r => r.startsWith(reportNamePrefix))
      dbutils.fs.ls(srcMount).map(_.name).foreach(println)
      extract_files.foreach(println)

      if (extract_files != null) {

        println(extract_files.mkString(","))
        val consumer_specific_path_suffix = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase()
        val archiveMount = if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
          s"dbfs:/mnt/$mainContainer/$archiveContainer/$consumer_specific_path_suffix/"
        }
        else {
          s"dbfs:/mnt/$mainContainer/$archiveContainer/"

        }

        println(s"archiveMount -> $archiveMount")
        for (filename <- extract_files) {
          if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos") && !dbutils.fs.ls(s"dbfs:/mnt/$mainContainer/$archiveContainer").map(_.name).contains(dbName.replace("CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase())) {
            dbutils.fs.mkdirs(archiveMount)

          }

          dbutils.fs.rm(srcMount + "/" + filename, recurse=true)
          print(" Deleted the file in the landing container: " + filename )
        }
      }
    }
    catch {
      case e: Throwable => println("No file found !   " + e)
    }
  }

  def archiveFiles(srcMount: String, reportNamePrefix: String) = {
    try {
      println(reportNamePrefix)
      println(" Source : " + srcMount)
      extract_files = dbutils.fs.ls(srcMount).map(_.name).filter(r => r.startsWith(reportNamePrefix))
      dbutils.fs.ls(srcMount).map(_.name).foreach(println)
      extract_files.foreach(println)

      if (extract_files != null) {

        println(extract_files.mkString(","))
        val consumer_specific_path_suffix = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase()
        val archiveMount = if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
          s"dbfs:/mnt/$mainContainer/$archiveContainer/$consumer_specific_path_suffix/"
        }
        else {
          s"dbfs:/mnt/$mainContainer/$archiveContainer/"

        }

        println(s"archiveMount -> $archiveMount")
        for (filename <- extract_files) {
          if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos") && !dbutils.fs.ls(s"dbfs:/mnt/$mainContainer/$archiveContainer").map(_.name).contains(dbName.replace("CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase())) {
            dbutils.fs.mkdirs(archiveMount)

          }

          dbutils.fs.mv(srcMount + "/" + filename, archiveMount)
          print(" Archived the file: " + filename + " to " + archiveMount)
        }
      }
    }
    catch {
      case e: Throwable => println("No file found !   " + e)
    }
  }

  // Archive Extracts
  def archiveExtracts(reportNamePrefix: String) = {

    //Move all old extract files from landing Container to Archive container
    val consumer_specific_path_suffix = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase()
    prefix = filePrefix + reportNamePrefix;

    landingMnt = if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
      // Example: Extracts the "wellmed" from "cdos_wellmed".
      // Extracts "arcadia" from "cdos_arcadia"
      val consumer_specific_path_suffix = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase()
      s"dbfs:/mnt/$mainContainer/$landingContainer/$consumer_specific_path_suffix/"
    }
    else {
      s"dbfs:/mnt/$mainContainer/$landingContainer/"
    }
    println(s"landingMnt is: $landingMnt")

    baseMnt = if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
      // Example: Extracts the "wellmed" from "cdos_wellmed".
      // Extracts "arcadia" from "cdos_arcadia"
      val consumer_specific_path_suffix = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "").toUpperCase()
      s"dbfs:/mnt/$mainContainer/$baseContainer/$consumer_specific_path_suffix/"
    }
    else {
      s"dbfs:/mnt/$mainContainer/$baseContainer/"
    }
    println(s"baseMnt is: $baseMnt")

    deleteFiles(landingMnt, prefix)
    archiveFiles(baseMnt, prefix)

  }
def mountContainer(containerName: String){

  val confKey = String.format("fs.azure.sas.%s.%s.blob.core.windows.net", containerName,azureStorageAccountName)
  val sasTokenKey = containerName + "-sastoken"
  
  val mount = "/mnt/" + containerName
  val sasToken = dbutils.secrets.get(sasTokenAccount, sasTokenKey)

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains(mount)){
  dbutils.fs.mount(
  source = String.format("wasbs://%s@%s.blob.core.windows.net", containerName,azureStorageAccountName),
  mountPoint = mount,
  extraConfigs = Map(confKey->sasToken))
}

}
def writeLargeFile(fileName: String, snowparkFileName: String, reportDF_final_escaped: DataFrame ) {

   reportDF_final_escaped.createOrReplaceTempView("reportDF_final_escaped")
    sfCDCM.sql("copy into " + snowparkFileName + " from reportDF_final_escaped overwrite=TRUE FILE_FORMAT = ( TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' FIELD_DELIMITER = '|' COMPRESSION = 'NONE' EMPTY_FIELD_AS_NULL = FALSE NULL_IF = '' ESCAPE_UNENCLOSED_FIELD = 'NONE' ) OVERWRITE = TRUE SINGLE = FALSE HEADER = TRUE").collect()

  mountContainer(mainContainer)
  val mountedDirectoryName = "/dbfs/mnt/" + mainContainer + "/" + landingContainer + "/"
  val shellScriptFileName = mountedDirectoryName + fileName
  
  val my_script = """
    combined_file="""" + shellScriptFileName + fileExtension + """"
    echo "Creating $combined_file..."
    # remove existing version
    rm -f $combined_file
    # get a list of files we want to combine
    list_of_files=($(ls """ + shellScriptFileName + """*.csv  | grep .*txt_[0-9].*))
    # Initialize the command that will be used to combine all the files into one with just one header row.
    my_cmd="cat  <(head -n 1 ""${list_of_files[0]}"")"
    for file in "${list_of_files[@]}"; do
        my_cmd+=" <(tail -n+2 ""$file"")"
    done
    my_cmd+=" > $combined_file"
    echo $my_cmd
    eval $my_cmd
    head "$combined_file"
    # Delete all of the single files...
    for file in "${list_of_files[@]}"; do
        rm "$file"
    done
    echo "Combined data saved to $combined_file" """

    val combineScriptFileName = "/mnt/" + mainContainer + "/" + landingContainer + "/" + fileName + "_cf.sh" 
    println("combineScriptFileName- "+combineScriptFileName)
    dbutils.fs.put(combineScriptFileName, my_script,true)
    val param = "/dbfs" + combineScriptFileName

    dbutils.notebook.run("combine-files", 3660, Map("FileName" -> param))
    dbutils.fs.rm(combineScriptFileName)
    
}

def generateExtractAndUploadInscope(reportName: String, reportDF: DataFrame) = {
    
    var consumer_specific_path_suffix = path_suffix.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "") .toUpperCase()
    println(s"Mount consumer_specific_path_suffix is: $consumer_specific_path_suffix")
    val snowflakeStage = "@" + sfDatabase + ".CAREDATA_EXTRACT_ELT." + mainContainer + "/" + landingContainer

    var originalFileName = ""
    var fileName = ""
    var snowparkFileName = ""
    if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
      originalFileName = s"$snowflakeStage/$consumer_specific_path_suffix/$filePrefix$reportName$spSuffix$fileExtension"
      fileName = s"$consumer_specific_path_suffix/$filePrefix$reportName$spSuffix"
      snowparkFileName = s"$snowflakeStage/$consumer_specific_path_suffix/$filePrefix$reportName$spSuffix$fileExtension"
    }
    else{
      originalFileName = s"$snowflakeStage/$filePrefix$reportName$spSuffix$fileExtension"
      fileName = s"$filePrefix$reportName$spSuffix"
      snowparkFileName = s"$snowflakeStage/$filePrefix$reportName$spSuffix$fileExtension"
    }
    println(s"originalFileName is: $originalFileName")
    //   Remove tab character anywhere in the column data (leading or trailing or in the middle)
    //   Removes spaces leading or trailing BUT NOT in the middle.
    //   Does not remove any other special characters.
    //   This logic is applied to the columns of String datatype only. If we apply to other data types, they will be converted to String type.
    val List_reportDF = reportDF.schema.fields.map(f =>(f.name, f.dataType))
    var reportDF_final = reportDF
    for (x <- List_reportDF) {
      val a = x._1
      val b = x._2
      if (b.typeName == "String") {
        reportDF_final = reportDF_final.withColumn(a, regexp_replace(com.snowflake.snowpark.functions.col(a),lit("^\\s+|\\s+$|\\t+ "),lit("")))
      }
      else {
        reportDF_final = reportDF_final.withColumn(a, com.snowflake.snowpark.functions.col(a))
      }
    }
    // Save the file
    if (reportDF_final.count == 0){
    //Snowpark won't write an empty dataframe to file, we need to make a dataframe with the header row as the data
    val colNames = StructType(reportDF_final.schema.map(f => StructField(f.name.toUpperCase , StringType)).toArray)
    val row = Seq(com.snowflake.snowpark.Row.fromSeq(reportDF_final.schema.map(f => f.name.toUpperCase).toSeq))
    val emptyDf = sfCDCM.createDataFrame(row, colNames)
    emptyDf
      .write
      .option("FIELD_DELIMITER","|")
      .option("HEADER", "FALSE")
      .option("SINGLE", "TRUE")
      .option("COMPRESSION","NONE")
      .option("ESCAPE_UNENCLOSED_FIELD", "NONE")
      .option("FIELD_OPTIONALLY_ENCLOSED_BY","NONE")
      .mode("overwrite")
      .csv(originalFileName)
	 
  }
  else{
    //Spark-like handling of double-quotes
    val allColumns = reportDF_final.schema.names.toSeq
    var reportDF_final_escaped = reportDF_final
    .withColumns(allColumns, allColumns
      .map(n => iff(contains(com.snowflake.snowpark.functions.col(n) ,lit("\""))
        , concat(lit("\""), regexp_replace(com.snowflake.snowpark.functions.col(n),lit("\""),lit("\\\\\"")), lit("\""))
        ,com.snowflake.snowpark.functions.col(n))))

      reportDF_final_escaped
        .write
        .option("FIELD_DELIMITER","|")
        .option("HEADER", "TRUE")
        .option("SINGLE", "TRUE")
        .option("COMPRESSION","NONE")
        .option("ESCAPE_UNENCLOSED_FIELD", "NONE")
        .option("FIELD_OPTIONALLY_ENCLOSED_BY","NONE")
        .option("NULL_IF","")
        .option("EMPTY_FIELD_AS_NULL", "FALSE")
        .option("max_file_size", 2147483647)
        .mode("overwrite")
        .csv(originalFileName)
}
  }
//Generate Extract and Upload to the Container

  def generateExtractAndUpload(reportName: String, reportDF: DataFrame ) = {

  var containerName = mainContainer+ "/"+landingContainer
  println("containerName- "+containerName)
var consumer_specific_path_suffix = path_suffix.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "") .toUpperCase()
    println(s"Mount consumer_specific_path_suffix is: $consumer_specific_path_suffix")
  val snowflakeStage = "@" + sfDatabase + ".CAREDATA_EXTRACT_ELT." + mainContainer + "/" + landingContainer
  println("SfSTAGE- "+snowflakeStage)
  if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
      fileName = s"$consumer_specific_path_suffix/$filePrefix$reportName$spSuffix"
      println("fileName- "+fileName)
      snowparkFileName = s"$snowflakeStage/$consumer_specific_path_suffix/$filePrefix$reportName$spSuffix$fileExtension"
      println("snowparkFileName- "+snowparkFileName)
    }
    else{
      fileName = s"$filePrefix$reportName$spSuffix"
      println("fileName- "+fileName)
      snowparkFileName = s"$snowflakeStage/$filePrefix$reportName$spSuffix$fileExtension"
      println("snowparkFileName- "+snowparkFileName)
    }
    //   Remove tab character anywhere in the column data (leading or trailing or in the middle)
    //   Removes spaces leading or trailing BUT NOT in the middle.
    //   Does not remove any other special characters.
    //   This logic is applied to the columns of String datatype only. If we apply to other data types, they will be converted to String type.
    val List_reportDF = reportDF.schema.fields.map(f=>(f.name, f.dataType))
    var reportDF_final = reportDF
    for (x <- List_reportDF) {
      val a = x._1
      val b = x._2.toString()
      if (b == "String") {
        reportDF_final = reportDF_final.withColumn(a, regexp_replace(com.snowflake.snowpark.functions.col(a),lit("^\\s+|\\s+$|\\t+ "),lit("")))
      }
      else {
        reportDF_final = reportDF_final.withColumn(a, com.snowflake.snowpark.functions.col(a))
      }
    }


    // Save the file
    if (reportDF_final.count == 0){
    //Snowpark won't write an empty dataframe to file, we need to make a dataframe with the header row as the data
    val colNames = StructType(reportDF_final.schema.map(f => StructField(f.name.toUpperCase , StringType)).toArray)
    val row = Seq(com.snowflake.snowpark.Row.fromSeq(reportDF_final.schema.map(f => f.name.toUpperCase).toSeq))
    val emptyDf = sfCDCM.createDataFrame(row, colNames)

    emptyDf
      .write
      .option("FIELD_DELIMITER","|")
      .option("HEADER", "FALSE")
      .option("SINGLE", "TRUE")
      .option("COMPRESSION","NONE")
      .option("ESCAPE_UNENCLOSED_FIELD", "NONE")
      .option("FIELD_OPTIONALLY_ENCLOSED_BY","NONE")
      .mode("overwrite")
      .csv(snowparkFileName)
	 
  }
   else{
    // Spark-like handling of double-quotes
   val allColumns = reportDF_final.schema.toSeq
    val reportDF_final_escaped = reportDF_final
      .withColumns(allColumns
        .map(n => n.name), allColumns.map(n => if (n.dataType.typeName!="String") col(n.name) else iff(contains(col(n.name),lit("\"")), concat(lit("\""), regexp_replace(col(n.name),lit("\""),lit("\\\\\"")), lit("\"")), col(n.name))))
    println(domainName)
    writeLargeFile(fileName, snowparkFileName, reportDF_final_escaped)

  }
  }

  // Updates the lastExtractDT column in the table
  def updateLastExtractDate() = {

    setCurrentDT()
    println("-----Updating last_extract_dt --------")
    val sqlQuery = s"""UPDATE """ + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") +""" set LAST_EXTRACT_DT ='""" + currentDT +"""' where FILE_NAME = '""" + domainName + """'"""
    print(sqlQuery)
    println(currentDT)
    sfCDCM.sql(sqlQuery).collect()
    val sql = s"""SELECT * from """ + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") + """ where  FILE_NAME = '""" + domainName + """'"""
    print(sql)
    val df = sfCDCM.sql(sql)
    println("------After Update-------")

  }
  def updateLastExtractDT() = {
    try {
      updateLastExtractDate()
    }
    catch {
      case _: Exception => Thread.sleep(100)
        println("updateLastExtractDt failed .. retry")
        updateLastExtractDate()
    }
  }

  // Generate History Extract
  def loadExtractsHistoryTable(reportName: String, reportDF: DataFrame) = {
    val loadTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.zzz").format(Calendar.getInstance().getTime())
    val loadDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())
    val histdbName = dbName.concat("_HISTORY_VALIDATION")
    val historyTable = histdbName.concat("_").concat(targetMarket).concat("_").concat(domainName).concat ("_hist") //  =< 1 year
    val archiveTable = histdbName.concat("_").concat(targetMarket).concat("_").concat(domainName).concat ("_archive") // 1 < x <= 2 year
    val historyDF = reportDF.withColumn("Filename", lit(reportName)).withColumn("LoadTime", lit(loadTimestamp)).withColumn("LoadDate", lit(loadDate))
    //   Remove tab character anywhere in the column data (leading or trailing or in the middle)
    //   Removes spaces leading or trailing BUT NOT in the middle.
    //   Does not remove any other special characters.
    //   This logic is applied to the columns of String datatype only. If we apply to other data types, they will be converted to String type.
    val List_reportDF = historyDF.schema.fields.map(f => (f.name, f.dataType))
    var reportDF_final = historyDF
    for (x <- List_reportDF) {
      val a = x._1
      val b = x._2
      if (b.typeName == "String") {
        reportDF_final = reportDF_final.withColumn(a,regexp_replace(col(a),lit("^\\s+|\\s+$|\\t+"),lit("")))
      }
      else {
        reportDF_final = reportDF_final.withColumn(a,col(a))
      }
    }

    //Save the Extract data into Delta lake table
    reportDF_final
    .write
    .mode(com.snowflake.snowpark.SaveMode.Append)
    .saveAsTable(historyTable);
    //Create _archive table IF NOT EXISTS
  val archiveDate = DateTime.now().minusDays(365)
  val archiveDateString = archiveDate.getYear().toString()
  .concat("%02d".format(archiveDate.getMonthOfYear).toString())
  .concat("%02d".format(archiveDate.getDayOfMonth).toString())
  
  var archiveDataDf = sfCDCM.sql("""select * from """ + historyTable + """ where LoadDate < """ + archiveDateString)
  
  archiveDataDf
    .write
       .mode(com.snowflake.snowpark.SaveMode.Append)
    .saveAsTable(archiveTable);  
  
  //Delete records older than 2 years from _archive table
  val deleteDate = DateTime.now().minusDays(730)
  val deleteDateString = deleteDate.getYear().toString()
  .concat("%02d".format(deleteDate.getMonthOfYear).toString())
  .concat("%02d".format(deleteDate.getDayOfMonth).toString())
  
  sfCDCM.sql("""delete from """ + archiveTable + """ where LoadDate < """ + deleteDateString).collect();
  
  //Delete records older than 1 year from _hist table
  sfCDCM.sql("""delete from """ + historyTable + """ where LoadDate < """ + archiveDateString).collect();
  }

  // Getting Inscope Patient DateTime Stamp to use it in all other notebooks
  def getExtractDate_inscope(targetMarket: String, dbName: String): String = {
    val sqlQuery = s"""SELECT LAST_EXTRACT_DT as lastExtractDT FROM """ + dbName.concat("_").concat(targetMarket).concat("_last_extract_info")+ """ where FILE_NAME = 'inscope_patient'"""
    val extractDt = sfCDCM.sql(sqlQuery).collect()(0)(0)
    println("extractDt:" + extractDt.toString())
    extractDt.toString()
  }

  // Updating the same value used in inscope-patient and migrating the value in the rest of the notebooks
  def updateLastExtractDate_exceptinscope() = {
    val extractDate_inscope = getExtractDate_inscope(targetMarket, dbName)
    println("extractDate_inscope for header updates: "+extractDate_inscope)
    val sqlQuery = s"""UPDATE """ + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") + """ set LAST_EXTRACT_DT = '""" + extractDate_inscope  + """'where FILE_NAME = '""" + domainName +  """'"""
    print(sqlQuery)
    sfCDCM.sql(sqlQuery).collect()
  }
  def updateLastExtractDT_exceptinscope() = {
    try {
      updateLastExtractDate_exceptinscope()
    } catch {
      case _: Exception => Thread.sleep(100)
        println("updateLastExtractDate_exceptinscope failed .. retry")
        updateLastExtractDate_exceptinscope()
    }
  }



def XwalkTableName(): String = {
var consumer_specific_path_suffix = ""
var sftable = ""
if (path_suffix.toUpperCase != "CAREDATA_EXTRACT_ELT.CDOS"){
consumer_specific_path_suffix = path_suffix.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos_", "") .toUpperCase()
}
else{
  consumer_specific_path_suffix = ""
}
println("consumer_specific_path_suffix: "+consumer_specific_path_suffix)
if (consumer_specific_path_suffix != "")
{
  sftable = "CAREDATA_EXTRACT_ELT.CDOS_DEFAULT_XWALK" + "_" + targetMarket+ "_" + consumer_specific_path_suffix
}
else
{
  sftable = "CAREDATA_EXTRACT_ELT.CDOS_DEFAULT_XWALK" + "_" + targetMarket
}
return sftable
}
 
  def defaultTable_insertion(targetMarket: String, domainName: String, reportDF: com.snowflake.snowpark.DataFrame): com.snowflake.snowpark.DataFrame = {
    import scala.collection.mutable.ListBuffer
    import scala.collection.mutable.ArrayBuffer
    import java.text.SimpleDateFormat
    import java.util.Calendar

    val currentTime = new SimpleDateFormat("yyMMdd_HHmmssSSS").format(Calendar.getInstance().getTime())
    println(currentTime)

    var temp_hist_table = "Temporary_History_Table_" + currentTime
    var orig_cols = reportDF.schema.map(x => x.name).toBuffer
    val sftable = XwalkTableName()
    println("sftable: " + sftable)

    var sqlDefaultCols = """
      SELECT 
      DISTINCT UPPER(SOURCE_ATTRIBUTE)
      FROM """ + sftable + """
      WHERE UPPER(ACTIVE_VALUE) = 'Y'
        AND LOWER(DOMAIN) = LOWER('""" + domainName + """')
        AND UPPER(TARGET_MARKET) = UPPER('""" + targetMarket + """')
    """

    var defaultColsDF = sfCDCM.sql(sqlDefaultCols)
    var Xwalk_DataFrame = sfCDCM.sql("""select * from """ + sftable)

    if (defaultColsDF.count > 0) {
      var default_cols = new ListBuffer[String]()
      for (row <- defaultColsDF.collect()) {
        var col = row.getString(0)
        default_cols += col
      }

      val new_cols = ArrayBuffer[String]()
      for (col <- orig_cols) {
        if (default_cols.contains(col)) {
          new_cols += col + "_NEW AS " + col
        } 
        else {
          new_cols += col
        }
      }

      var newColList = new_cols.mkString(", ")
      var sql1 = XwalkdefaultQueryResult(default_cols, Xwalk_DataFrame)
      var sql2 = "select " + newColList + " from (select *" + sql1 + " from "+temp_hist_table+")"
      println(sql2)
      reportDF.createOrReplaceTempView(temp_hist_table)
      var new_df = sfCDCM.sql(sql2)
      new_df
    } 
    else {
      reportDF
    }
  }

  def XwalkdefaultQueryResult(source_attrs: Seq[String], Xwalk_DataFrame: DataFrame): String = {
    var resultvar = ""
    for (x <- source_attrs) {

      var str = ", case "
      for (row <- Xwalk_DataFrame.filter(regexp_replace(col("source_attribute"), lit(" "), lit("")) === lit(x)).collect()) {
        val tmp: String = if (row.get(2).toString.toLowerCase != "all") {
          "source_system_sk in (" + row.get(2)
        } + ") and " else ""

        val str1 =  " when " + tmp

        val then_variable = " then "
        if (row.get(6).toString.trim.toLowerCase() == "null" || row.get(6).toString.trim.length == 0) {
          str = str + str1 +
            row.get(4) + " is " + row.get(6).toString.toLowerCase() + then_variable + row.get(7)

          str = str + str1 +
            "length(trim(" + row.get(4) + ")) = 0 then " +  row.get(7)

          str = str + str1 +
            row.get(4) + "='\\n' then " + row.get(7)
        }
        else if(row.get(6).toString.trim.toLowerCase() != "null" && row.get(6).toString.trim.length != 0 && row.get(9).toString.trim.toLowerCase() == "<") {
          str = str + str1 +
            row.get(4) + " < " + row.get(6) + then_variable + row.get(7)
        }
        else if(row.get(6).toString.trim.toLowerCase() != "null" && row.get(6).toString.trim.length != 0 && row.get(9).toString.trim.toLowerCase() == ">") {
          str = str + str1 +
            row.get(4) + " > " + row.get(6) + then_variable + row.get(7)
        }
        else {
          str = str + str1 +
            row.get(4) + " = " + row.get(6) + then_variable + row.get(7)
        }
      }
      str = str + " else " + x + " end as " + x + "_new "
      println(str)
      resultvar = resultvar + str
    }
    return resultvar
  }

//Parse Validate Date (Used in Eligibility)

  import java.text.ParseException

  def validateDate(Record_Start_Date: String) =
    try {
      val df = new SimpleDateFormat("yyyy-MM-dd")
      df.setLenient(false)
      df.parse(Record_Start_Date)
      true
    }
    catch {
      case e: ParseException => false
    }

  def dropTableifExists(tableName: String) {
    sfCDCM.sql("DROP TABLE IF EXISTS " + tableName).collect()
  }

def getDataFromSnowflakeTable(tableName: String): DataFrame = {
val df = sfCDCM.sql("select * from "+tableName)
df
}
  def writeDataFrameToSnowflake(DF: DataFrame, tableName: String, mode: String) {
    // DBTITLE 1,Write to the Delta Lake Table
    println("Loading into "+tableName)
    if (mode.toLowerCase == "overwrite")
    {
    DF.write
      .mode(com.snowflake.snowpark.SaveMode.Overwrite)
      .saveAsTable(tableName)
    }
    else
    {
       DF.write
      .mode(com.snowflake.snowpark.SaveMode.Append)
      .saveAsTable(tableName)
    }
  }

   def dropcolumnsfromDataframe(DF:DataFrame, cols:Seq[String]) :DataFrame = {
    println("dropcolumnsfromDataframe:"+cols.toString())
    var DF1 = DF
    for (i <- cols)
    DF1 = DF1.drop(i)
    val newDF = DF1
    val schema = newDF.schema
    print(schema)
    newDF
  }
  def runSnowflakeQuery(query: String) {
    println("runSnowflakeQuery:",query)

    Utils.runQuery(sfOptions.asJava,query)
  }

  def getDataframeFromSnowflake(query: String): DataFrame = {
    val Df = sfCDCM.sql(query)
    Df
    }

  def generateAndLoadExtractUploadHistory_Delta(TableDF_history: DataFrame, TableDF_Delta: DataFrame, reportNamePrefix: String): String = {
    generateExtractAndUpload(reportNamePrefix, TableDF_Delta)
    val reportName = reportNamePrefix
    val valReportName = reportName + ".txt"
    // Creates two tables history and archive
    loadExtractsHistoryTable(valReportName, TableDF_history) // Using Source_system_sk and Tenant_sk
    // Update the Last Extract Date same as Inscope Patient Extract Date
    updateLastExtractDT_exceptinscope()
    return " Success!"

  }

  def filename_extraction(tenant:String,target_market:String): String = {
    val file_type = "csv"
    val fileName_DF = spark.read.format(file_type)
      .option("ïnferSchema", "false")
      .option("header", "true")
      .option("delimiter","|")
      .load(filename_location)

    val df = fileName_DF
      .filter(org.apache.spark.sql.functions.col("tenant") === org.apache.spark.sql.functions.lit(tenant))
      .filter(org.apache.spark.sql.functions.col("target_market") === org.apache.spark.sql.functions.lit(target_market)).select(org.apache.spark.sql.functions.col("filename"))
    print(tenant)
    print(target_market)

    val file_name=if (df.count() > 0  ) {
      df.rdd.map(x=>x(0).toString).collect()(0)
    }
    else {""}
    file_name

  }

  consumer = if (!dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
    dbName.replace("CAREDATA_EXTRACT_ELT.cdos_", "")
  } else {
    ""
  }

  def mkdirectory(dataBaseName: String) = ignoring(classOf[java.lang.ExceptionInInitializerError]) {

    // dbutils throws errors running locally outside of the cluster- swallow these
    try {
      if (dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
        query_directory = s"/FileStore/users/cdos/queries/${targetMarket.toLowerCase}"
        dbutils.fs.mkdirs(query_directory)
      } else {
        query_directory = s"/FileStore/users/cdos/queries/${targetMarket.toLowerCase.concat("-").concat(consumer.toString)}"
        dbutils.fs.mkdirs(query_directory)
      }

    } catch {
      case _: Throwable =>
    }
  }

  mkdirectory(dbName)

  def query_writer(targetMarket: String, domainName: String, consumer: String, query: String) = {
    val target_domainName = if (dbName.equalsIgnoreCase("CAREDATA_EXTRACT_ELT.cdos")) {
      targetMarket.toLowerCase.concat("-").concat(domainName)
    } else {
      targetMarket.toLowerCase.concat("-").concat(consumer.toString).concat("-").concat(domainName)
    }

    try {
      Files.write(Paths.get(s"/dbfs/$query_directory/$target_domainName-query.txt"), query.getBytes("ISO-8859-1"))
    } catch {
      case e: Throwable => println("Error in writing query to file")
    }

  }



  // Updates the last_extract_info table with the previous extractDT for the given market and domain.
  val updatePreviousExtractDate = (lastExtractDT: String, targetMarket: String, domain: String) => {
    println("domain: "+domain)
    try {
      println("domain: "+domain)
      sfCDCM.sql(String.format("Update " + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") + " SET PREVIOUS_LAST_EXTRACT_DT = '%s' WHERE FILE_NAME='%s'", lastExtractDT, domain)).collect()
      true
    } catch {
      case _: Exception => Thread.sleep(100)
        println("updatePreviousExtractDate failed ... retry")
        println("domain: "+domain)
        sfCDCM.sql(String.format("Update " + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") + " SET PREVIOUS_LAST_EXTRACT_DT = '%s' WHERE FILE_NAME='%s'", lastExtractDT, domain)).collect()
        true
    }
  }

  def updateLastExtractDate_exceptinscope_medclaim_details() = {
    val extractDate_inscope = getExtractDate_inscope(targetMarket, dbName)
    val sqlQuery = s"""UPDATE """ + dbName.concat("_").concat(targetMarket).concat("_last_extract_info") + """ set LAST_EXTRACT_DT = '""" + extractDate_inscope + """'where FILE_NAME = 'medclaim_details'"""
    print(sqlQuery)
    sfCDCM.sql(sqlQuery).collect()
  }
  def loadClaimDetailsExtractsHistoryTable(reportName: String, reportDF: DataFrame) = {
    val loadTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.zzz").format(Calendar.getInstance().getTime())
    val loadDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())
    val histdbName = dbName.concat("_HISTORY_VALIDATION")
    val detailshistoryTable = histdbName.concat("_").concat(targetMarket).concat("_medclaim_details_hist") //  =< 1 year
    val detailsarchiveTable = histdbName.concat("_").concat(targetMarket).concat("_medclaim_details_archive") // 1 < x <= 2 year
    val historyDF = reportDF.withColumn("Filename", lit(reportName)).withColumn("LoadTime", lit(loadTimestamp)).withColumn("LoadDate", lit(loadDate))
    //   Remove tab character anywhere in the column data (leading or trailing or in the middle)
    //   Removes spaces leading or trailing BUT NOT in the middle.
    //   Does not remove any other special characters.
    //   This logic is applied to the columns of String datatype only. If we apply to other data types, they will be converted to String type.
    val List_reportDF = historyDF.schema.fields.map(f => (f.name, f.dataType))
    var reportDF_final = historyDF
    for (x <- List_reportDF) {
      val a = x._1
       val b = x._2
      if (b.typeName == "String") {
        reportDF_final = reportDF_final.withColumn(a,regexp_replace(col(a), lit("^\\s+|\\s+$|\\t+"),lit("")))
      }
      else {
        reportDF_final = reportDF_final.withColumn(a,col(a))
      }
    }

    //Save the Extract data into Delta lake table
    reportDF_final.write.mode(com.snowflake.snowpark.SaveMode.Append).saveAsTable(detailshistoryTable);
    //Create _archive table IF NOT EXISTS
   val archiveDate = DateTime.now().minusDays(365)
   val archiveDateString = archiveDate.getYear().toString()
   .concat("%02d".format(archiveDate.getMonthOfYear).toString())
   .concat("%02d".format(archiveDate.getDayOfMonth).toString())
  
   var archiveDataDf = sfCDCM.sql("""select * from """ + detailshistoryTable + """ where LoadDate < """ + archiveDateString)
  
  archiveDataDf
  .write
  .mode(com.snowflake.snowpark.SaveMode.Append)
  .saveAsTable(detailsarchiveTable);  
  
  //Delete records older than 2 years from _archive table
  val deleteDate = DateTime.now().minusDays(730)
  val deleteDateString = deleteDate.getYear().toString()
  .concat("%02d".format(deleteDate.getMonthOfYear).toString())
  .concat("%02d".format(deleteDate.getDayOfMonth).toString())
  
  sfCDCM.sql("""delete from """ + detailsarchiveTable + """ where LoadDate < """ + deleteDateString).collect();
  
  //Delete records older than 1 year from _hist table
  sfCDCM.sql("""delete from """ + detailshistoryTable + """ where LoadDate < """ + archiveDateString).collect();
  }


  //UDF to filter values for PROVIDER_TIN,LINE_OF_BUSINESS, BUSINESS_TYPE

  def filterReferenceList_Exclusion(targetMarket: String, domain: String, filterReference: String, consumer: String): String = {
    //reference can be LINE_OF_BUSINESS,BUSINESS_TYPE
    var exclusion_list: String = ""
    val file_type = "csv"
    val exclusion_DF = spark.read.format(file_type)
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .option("header", "true")
      .load(filterationTablefile_location)

    val activeFlag = "Y"
    val exclusion_DataFrame_Filtered = exclusion_DF
      .filter(org.apache.spark.sql.functions.col("active_flg") === org.apache.spark.sql.functions.lit(activeFlag))
      .filter(org.apache.spark.sql.functions.col("reference") === org.apache.spark.sql.functions.lit(filterReference.toUpperCase()))
      .filter(org.apache.spark.sql.functions.col("domain") === org.apache.spark.sql.functions.lit(domain.toLowerCase().capitalize))
      .filter((org.apache.spark.sql.functions.col("consumer")) === org.apache.spark.sql.functions.lit(consumer.toUpperCase()) || org.apache.spark.sql.functions.col("consumer") === org.apache.spark.sql.functions.lit("ALL"))


    //Precedence to Market specific filter over All markets for filterReference = LINE_OF_BUSINESS and BUSINESS_TYPE
    val targetMarketFilter = exclusion_DataFrame_Filtered.filter((org.apache.spark.sql.functions.col("market") === org.apache.spark.sql.functions.lit(targetMarket.toUpperCase)))
    var allTargetMarketFIlter: org.apache.spark.sql.DataFrame = spark.emptyDataFrame
    if (targetMarketFilter.count() == 0) {
      allTargetMarketFIlter = exclusion_DataFrame_Filtered.filter((org.apache.spark.sql.functions.col("market") === "ALL"))
    } else allTargetMarketFIlter = exclusion_DataFrame_Filtered.filter(org.apache.spark.sql.functions.col("market") === "ALL" || org.apache.spark.sql.functions.col("market") === org.apache.spark.sql.functions.lit(targetMarket.toUpperCase))

    if (allTargetMarketFIlter.count() > 0) {
      val source_attrs = allTargetMarketFIlter.select(org.apache.spark.sql.functions.col("value_exclusion")).rdd.map(row => Option(row.getAs[String](0)).getOrElse("")).distinct().collect().toSeq

      source_attrs.foreach(println)
      exclusion_list = source_attrs.mkString(",")
    }
    //Return provider list with comma seperated single quotes.
    exclusion_list.split(",").mkString("'", "', '", "'")
  }

  def filterReferenceList_Inclusion(targetMarket: String, domain: String, filterReference: String, consumer: String): String = {
    //reference can be LINE_OF_BUSINESS,BUSINESS_TYPE
    var inclusion_list: String = ""
    val file_type = "csv"
    val inclusion_DF = spark.read.format(file_type)
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .option("header", "true")
      .load(filterationTablefile_location)

    val activeFlag = "Y"
    val inclusion_DataFrame_Filtered = inclusion_DF
      .filter(org.apache.spark.sql.functions.col("active_flg") === org.apache.spark.sql.functions.lit(activeFlag))
      .filter(org.apache.spark.sql.functions.col("reference") === org.apache.spark.sql.functions.lit(filterReference.toUpperCase()))
      .filter(org.apache.spark.sql.functions.col("domain") === org.apache.spark.sql.functions.lit(domain.toLowerCase().capitalize))
      .filter(org.apache.spark.sql.functions.col("value_inclusion") =!= "")
      .filter((org.apache.spark.sql.functions.col("consumer")) === org.apache.spark.sql.functions.lit(consumer.toUpperCase()) || org.apache.spark.sql.functions.col("consumer") === org.apache.spark.sql.functions.lit("ALL"))

    //Precedence to Market specific filter over All markets for filterReference = LINE_OF_BUSINESS and BUSINESS_TYPE
    val targetMarketFilter = inclusion_DataFrame_Filtered.filter((org.apache.spark.sql.functions.col("market") ===org.apache.spark.sql.functions.lit(targetMarket.toUpperCase)))
    var allTargetMarketFIlter: org.apache.spark.sql.DataFrame = spark.emptyDataFrame
    
    if (targetMarketFilter.count() == 0) {
      allTargetMarketFIlter = inclusion_DataFrame_Filtered.filter((org.apache.spark.sql.functions.col("market") === "ALL"))
    } else allTargetMarketFIlter = inclusion_DataFrame_Filtered.filter(org.apache.spark.sql.functions.col("market") === "ALL" || org.apache.spark.sql.functions.col("market") ===org.apache.spark.sql.functions.lit(targetMarket.toUpperCase))

    if (allTargetMarketFIlter.count() > 0) {
      val source_attrs = allTargetMarketFIlter.select(org.apache.spark.sql.functions.col("value_inclusion")).rdd.map(row => Option(row.getAs[String](0)).getOrElse("")).distinct().collect().toSeq
      source_attrs.foreach(println)
      inclusion_list = source_attrs.mkString(",")
    }
    //Return provider list with comma seperated single quotes.
    inclusion_list.split(",").mkString("'", "', '", "'")
  }

    def sdm_extraction(target_market:String): String = {

    val file_type = "csv"
    val fileName_DF = spark.read.format(file_type)
      .option("ïnferSchema", "false")
      .option("header", "true")
      .option("delimiter","|")
      .load(filename_location)
    val df = fileName_DF
      .filter(org.apache.spark.sql.functions.col("target_market") === org.apache.spark.sql.functions.lit(target_market)).select(org.apache.spark.sql.functions.col("sdm_type"))
    val sdm=if (df.count() > 0  ) {
      df.rdd.map(x=>x(0).toString).collect()(0)
    }
    else {""}
    sdm
  }

   def eid_applied(target_market:String): String = {

    val file_type = "csv"
    val fileName_DF = spark.read.format(file_type)
      .option("ïnferSchema", "false")
      .option("header", "true")
      .option("delimiter","|")
      .load(filename_location)
    val df = fileName_DF
      .filter(org.apache.spark.sql.functions.col("target_market") === org.apache.spark.sql.functions.lit(target_market)).select(org.apache.spark.sql.functions.col("eid_applied"))
    val eid=if (df.count() > 0  ) {
      df.rdd.map(x=>x(0).toString).collect()(0)
    }
    else {""}
    eid

  }
  def dropSnowflakeTable(tableName: String): Unit = {
  sfCDCM.sql(s"DROP TABLE IF EXISTS $tableName").collect()
}  
    def market_extraction(sdm_type: String): List[String] = {
    val file_type = "csv"
    val fileName_DF = spark.read.format(file_type)
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter","|")
      .load(filename_location)
    val df = fileName_DF
      .filter(org.apache.spark.sql.functions.col("sdm_type") === org.apache.spark.sql.functions.lit(sdm_type))
      .select("target_market")
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
    df
  }
  def SrcTableName(domain: String): String = {
		val SrcTableNameMap = Map[String, String](
	"eligibility" -> "eligibility_source"
  ,"empi" -> "empi_source"
  ,"inscope_patient" -> "inscope_patient"
  ,"lab_result" -> "lab_result_source"
  ,"location" -> "location_source"
  ,"medclaim_details" -> "medclaim_details_source"
  ,"medclaim_header" -> "medclaim_header_source"
  ,"patient" -> "patient_source"
   ,"incr_mbrhicnhistory" -> "patienthicnmbi"
  , "pharmacy" -> "pharmacy_source"
  ,"pod" -> "pod_source"
  ,"provider" -> "provider_source"
  ,"provider_group" -> "provider_group_source"
		)
    var d = domain.toLowerCase
		return SrcTableNameMap(d)
	}
def getTimeTravel(domainName: String, targetMarket: String) : String = {
  var featureName = ""
  if(consumer ==""){
    featureName=""
  }else{
    featureName=dbName.split("(?i)cdos").last
  }
  var sparkSourceTableName = targetMarket.toLowerCase + "_" + SrcTableName(domainName)
  var tableExists = spark.sql("show tables in cdos" + featureName).filter("tableName like '%" + sparkSourceTableName + "%'").count()
  //DEV, STG, SIT envs have 1 day time travel, PRD has 90 days
  var timeTravelLimit = "23"
  if (sfEnv == "PRD") {timeTravelLimit = "2159"}

     val sql = """select timestamp from
    (select operation,max(timestamp) AS timestamp from (DESCRIBE HISTORY cdos""" + featureName +
    """.""" + sparkSourceTableName + """)
    where operation LIKE 'CREATE OR REPLACE TABLE%'
    GROUP BY operation)
    where timestamp + INTERVAL '""" + timeTravelLimit + """' HOUR > current_timestamp()
    """

  var timeTravel = ""

  if (tableExists > 0) 
  {
    val sourceTableCreateTimeDF = spark.sql(sql)
    if (sourceTableCreateTimeDF.count() > 0){
    val sourceTableCreateTime = sourceTableCreateTimeDF.select("timestamp").collect()(0)(0)

    timeTravel = " AT(TIMESTAMP => '" + sourceTableCreateTime + "00 +0000'::TIMESTAMP_TZ) "
    }
  }

timeTravel
}
def getDateForLookbackPeriod (timeTravel: String) : String = {
  var timeTravelDate = ""
  val currentMonth = LocalDate.now().getMonthValue.toString
  println("currentMonth: " + currentMonth)

  if (timeTravel.nonEmpty) {
    if (currentMonth != timeTravel.substring(23, 25)) {
      timeTravelDate = "to_date('" + timeTravel.substring(18, 28) + "')"}
    else {
      timeTravelDate = "getdate()"} }

  else {
    timeTravelDate = "getdate()"}

  timeTravelDate

}

def deleterecordsfromSnowflake(consumername:String = "") = {
    val histdbName = dbName.concat("_HISTORY_VALIDATION")
    val market =  targetMarket.toLowerCase()

    var tblSeq = Seq("LAB", "LOCATION", "MEMBER","ELIGIBILITY", "EMPI_LPI_CROSSWALK", "MEMBER_HICN", "CLAIM_DETAIL", "CLAIM_HEADER", "PHARMACY", "POD", "PROVIDER", "PROVIDER_GROUP")
    tblSeq.foreach {
      (domainName: String) =>
      val deleteQuery = s""" DELETE FROM CDOS_EXTRACT.$domainName where _CONSUMER ='$consumername' and _MARKET = '$market' """
      println(deleteQuery)
      sfCDCM.sql(deleteQuery).collect()
      println("Records delete completed for "+domainName+" Market "+market)
    }
}

}
// End of File
