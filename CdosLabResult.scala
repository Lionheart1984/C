// Databricks notebook source
// MAGIC %run ./CdosBase

// COMMAND ----------

// MAGIC %run ./CdosConstants

// COMMAND ----------

// MAGIC %run ./CdosConfigMap

// COMMAND ----------

import com.snowflake.snowpark.{DataFrame=>SpDataFrame}
import com.snowflake.snowpark._
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Session
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar


import scala.language.{postfixOps, reflectiveCalls}


class CdosLabResult (var notebookParams: scala.collection.mutable.Map[String, String], sfCDCM:Session) extends SparkSessionWrapper {
  // Variable declarations
  val domainName = CdosConstants.cdosLabResultDomainName
  var cdosBase = new CdosBase(notebookParams, sfCDCM)
  notebookParams += ("domainName" -> "LAB_RESULT")
  notebookParams += ("consumer" -> cdosBase.consumer)
  cdosBase.domainName = domainName
  var cdosConfigMap = new CdosConfigMap(notebookParams, sfCDCM)
  var targetMarket: String = notebookParams("targetMarket").toLowerCase()
  var sourceSystem: String = notebookParams("sourceSystem")
  var lookbackPeriod: String = notebookParams("lookbackPeriod")
  var dbName: String = notebookParams("dbName")
  val RunType: String = notebookParams("RunType")
  var targetQuery: String = null
  var timeTravel=""
  var timeTravelDate = ""
  val useSparkLastExtractForTimeTravel=notebookParams("useSparkLastExtractForTimeTravel")
  if (useSparkLastExtractForTimeTravel == "true")
    {timeTravel = cdosBase.getTimeTravel(domainName, targetMarket)}
  println(timeTravel)
  timeTravelDate = cdosBase.getDateForLookbackPeriod(timeTravel)
  println("timeTravelDate: " + timeTravelDate)
  val labresultTable: String = cdosBase.dbName.concat("_").concat(cdosBase.targetMarket).concat("_").concat(domainName)
  var labresultSourceTable: String = cdosBase.dbName.concat("_").concat(cdosBase.targetMarket).concat("_").concat(domainName).concat("_source")
  var lastExtractDate = LocalDate.now().plusMonths(-39).format(DateTimeFormatter.ofPattern("yyMMdd"))
  var currentDate = new SimpleDateFormat("yyMMdd").format(Calendar.getInstance().getTime())

  if (notebookParams("targetMarket") != null)
    targetMarket = notebookParams("targetMarket").toLowerCase()

  if (notebookParams("lookbackPeriod") != null)
    lookbackPeriod = notebookParams("lookbackPeriod")

  var tenant = notebookParams("tenant")
  val TENANT_NUMBER = "(" + tenant + ")"

  val labresultSourceQuery =
    """
          with cte_hum
          as
          (
            select distinct
            P.Source_Patient_ID as Hum_id,
            P.Source_Patient_ID
            from  """ + cdosBase.sfSchema + """.LAB_RESULT """ + timeTravel + """ L
             inner join """ + cdosBase.sfSchema + """.PATIENT """ + timeTravel + """ P on L.Patient_SK = P.Patient_SK
            union all
            select distinct
            p.Source_Patient_ID as Hum_id,
            cc.source_patient_id
            from  """ + cdosBase.sfSchema + """.LAB_RESULT """ + timeTravel + """ L
            inner join  """ + cdosBase.sfSchema + """.PATIENT """ + timeTravel + """ p on L.patient_sk = p.patient_sk
            inner join cdcm_elt.patient_sbr_crosswalk """ + timeTravel + """ c on p.source_patient_id = c.source_patient_id
            inner join cdcm_elt.patient_sbr_crosswalk """ + timeTravel + """ cc on c.enterprise_id = cc.enterprise_id and cc.source='OCSP_FACETS'
            where L.source_system_sk=10005
           )
      select
      TENANT_SK, SOURCE_SYSTEM_SK, SOURCE_SYSTEM, CONTRACT, PBP, ENTITY_MEMBER_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, PATIENT_BIRTH_DATE, PATIENT_MBI_OR_HICN, LAB_ORDER_NUMBER, LAB_RESULT_NAME, LAB_RESULT_OBSERVATION_DATE, LAB_RESULT_OBSERVATION_TIME, LAB_RESULT_VALUE, LAB_RESULT_UNIT_OF_MEASURE, LAB_RESULT_REFERENCE_RANGE, LAB_RESULT_OBSERVE_STATUS, LAB_RESULT_COMMENT, LAB_RESULT_TEST_NAME, LAB_RESULT_TYPE, LAB_RESULT_LOINC_CODE,  LAB_RESULT_CPT_CODE,LAB_RESULT_FILLER_ORDER_NUMBER, LAB_RESULT_REQUESTED_DATE_TIME, LAB_RESULT_ORDERED_DATE_TIME, LAB_RESULT_OBSERVATION_REQUEST_TIME, LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS, LAB_RESULT_SPECIMEN_SOURCE, LAB_RESULT_ABNORMAL_FLAG, LAB_RESULT_NATURE_OF_ABNORMAL_TEST, LAB_RESULT_SK, DEL_IND, HASH
      from (
        Select *, row_number() OVER (  partition by TENANT_SK, SOURCE_SYSTEM_SK,ENTITY_MEMBER_ID,LAB_ORDER_NUMBER,LAB_RESULT_NAME,
                           LAB_RESULT_OBSERVATION_DATE,LAB_RESULT_TEST_NAME,LAB_RESULT_LOINC_CODE
                           order by LAB_RESULT_SEQUENCE desc, LAB_RESULT_SK desc) AS Row_Num
        from
         (
          SELECT DISTINCT
              LR.TENANT_SK,
              LR.SOURCE_SYSTEM_SK,
              SS.SOURCE_SYSTEM_NAME as SOURCE_SYSTEM,
              '0' as CONTRACT,
              '0' as PBP,
              P.SOURCE_PATIENT_ID AS ENTITY_MEMBER_ID,
              (FIRST_NAME) as FIRST_NAME,
              (P.MIDDLE_NAME) as MIDDLE_NAME,
              (LAST_NAME) as LAST_NAME,
              DATE(P.BIRTH_DATE) as PATIENT_BIRTH_DATE ,
              OCDP_SECURITY.DPAAS_DETOKEN_HICN_MBI(P.HICN_MBI) AS PATIENT_MBI_OR_HICN,
              LR.LAB_ORDER_ID AS LAB_ORDER_NUMBER ,
              nvl(substring(trim(LR.LAB_ORDER_DESC),1,128),'') as LAB_RESULT_NAME ,
              """ + cdosConfigMap.configuration_map("IN_LAB_ORDER_CONDITION") + """,
              SUBSTRING(CAST(LR.LAB_ORDER_PERF_DT AS VARCHAR),12,12) AS LAB_RESULT_OBSERVATION_TIME ,
              LR.LAB_RESULT_VALUE_RAW as LAB_RESULT_VALUE ,
              LR.LAB_RESULT_VALUE_UOM AS LAB_RESULT_UNIT_OF_MEASURE ,
              CASE WHEN LR.RESULT_REF_RANGE IS NULL OR TRIM(LR.RESULT_REF_RANGE) = '' THEN '0-0'
              ELSE LR.RESULT_REF_RANGE END AS LAB_RESULT_REFERENCE_RANGE  ,
              NVL(LR.RESULT_STATUS,'F') AS LAB_RESULT_OBSERVE_STATUS ,
              substring(TRIM(LR.RESULT_COMMENT),1,255) AS LAB_RESULT_COMMENT ,
              substring(TRIM(NVL(LR.LAB_ORDER_TEST_NAME,NVL(LR.LAB_ORDER_DESC,''))),1,100) as LAB_RESULT_TEST_NAME ,
              LR.LAB_ORDERABLE_CD AS LAB_RESULT_TYPE ,
              LR.LOINC_CD AS LAB_RESULT_LOINC_CODE ,
              LR.CPT_CD AS LAB_RESULT_CPT_CODE ,
              LR.ACCESSION_NUMBER AS LAB_RESULT_FILLER_ORDER_NUMBER ,
              CAST(LR.ORDER_REQUEST_DT AS VARCHAR) AS LAB_RESULT_REQUESTED_DATE_TIME ,
              CAST(LR.LAB_ORDERED_DT AS VARCHAR) AS LAB_RESULT_ORDERED_DATE_TIME ,
              CAST(LR.OBSERVATION_REQUEST_DT AS VARCHAR) AS LAB_RESULT_OBSERVATION_REQUEST_TIME ,
              'F' AS LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS ,
              LR.SPECIMEN_SOURCE_DESC AS LAB_RESULT_SPECIMEN_SOURCE ,
              LR.ABNORMAL_STATUS_CD AS LAB_RESULT_ABNORMAL_FLAG ,
              LR.ABNORMAL_TEST_NATURE AS LAB_RESULT_NATURE_OF_ABNORMAL_TEST ,
              LR.LAB_RESULT_SK as LAB_RESULT_SK ,
              cast(LR.DEL_IND as string) as DEL_IND,
              LR.LAB_RESULT_SEQUENCE,
              hash(SOURCE_SYSTEM,CONTRACT,PBP , ENTITY_MEMBER_ID , FIRST_NAME , MIDDLE_NAME, LAST_NAME,  PATIENT_BIRTH_DATE ,  P.HICN_MBI,
              LAB_ORDER_NUMBER , LAB_RESULT_NAME , LAB_RESULT_OBSERVATION_DATE , LAB_RESULT_OBSERVATION_TIME , LAB_RESULT_VALUE , LAB_RESULT_UNIT_OF_MEASURE ,
              LAB_RESULT_REFERENCE_RANGE , LAB_RESULT_OBSERVE_STATUS , LAB_RESULT_COMMENT , LAB_RESULT_TEST_NAME , LAB_RESULT_TYPE , LAB_RESULT_LOINC_CODE ,
              LAB_RESULT_CPT_CODE , LAB_RESULT_FILLER_ORDER_NUMBER , LAB_RESULT_REQUESTED_DATE_TIME , LAB_RESULT_ORDERED_DATE_TIME , LAB_RESULT_OBSERVATION_REQUEST_TIME ,
              LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS , LAB_RESULT_SPECIMEN_SOURCE , LAB_RESULT_ABNORMAL_FLAG , LAB_RESULT_NATURE_OF_ABNORMAL_TEST , LR.DEL_IND) as HASH
              FROM """ + cdosBase.sfSchema + """.LAB_RESULT """ + timeTravel + """ LR
              LEFT JOIN """ + cdosBase.sfSchema + """.SOURCE_SYSTEM """ + timeTravel + """ SS
              ON LR.SOURCE_SYSTEM_SK=SS.SOURCE_SYSTEM_SK
              INNER JOIN """ + cdosBase.sfSchema + """.PATIENT """ + timeTravel + """ P
              ON P.PATIENT_SK = LR.PATIENT_SK
              INNER JOIN cte_hum CW
              ON P.SOURCE_PATIENT_ID = CW.hum_id
              WHERE
              LR.TENANT_SK IN """ + TENANT_NUMBER + """
                AND P.DEL_IND = 0 AND LR.DEL_IND = 0 AND SS.DEL_IND = 0
                """ + cdosConfigMap.configuration_map("IN_LAB_ORDER_2_CONDITION") + """
                AND LR.LOINC_CD IS NOT NULL AND LR.LOINC_CD LIKE '%-%'
                AND LR.AUD_UPD_DT >= to_timestamp_ntz(dateadd('month', -""" + lookbackPeriod + """, date_trunc('month', """ + timeTravelDate + """)))
                )
                 ) a
                 where a.Row_Num = '1'
                """
  println(cdosBase.targetMarket)
  println("")
  println(cdosConfigMap.sdm_type)
  println("")
  println(cdosConfigMap.eid)
  println("")
  if ("SDM2".equals(cdosConfigMap.sdm_type)) {
    targetQuery = labresultSourceQuery
  }
  else if ("SDM3".equals(cdosConfigMap.sdm_type)) {
    targetQuery = labresultSourceQuery
  }
  if (sourceSystem != "All") {
    targetQuery = labresultSourceQuery + """ AND SOURCE_SYSTEM_SK IN ( """ + sourceSystem + """) """

  }

  cdosBase.query_writer(targetMarket.toLowerCase, domainName,cdosBase.consumer, targetQuery)

  val labresultDF: String =
    """ Select
      LR.TENANT_SK ,
      LR.SOURCE_SYSTEM_SK,
      LR.LAB_RESULT_SK,
      LR.SOURCE_SYSTEM,
      LR.CONTRACT,
      LR.PBP,
      LR.ENTITY_MEMBER_ID,
      TRIM(OCDP_SECURITY.DPAAS_DETOKEN_NAME(LR.FIRST_NAME)) AS FIRST_NAME,
      CASE WHEN TRIM(OCDP_SECURITY.DPAAS_DETOKEN_NAME(LR.MIDDLE_NAME)) = '' THEN NULL ELSE TRIM(OCDP_SECURITY.DPAAS_DETOKEN_NAME(LR.MIDDLE_NAME)) END AS MIDDLE_NAME,
      TRIM(OCDP_SECURITY.DPAAS_DETOKEN_NAME(LR.LAST_NAME)) AS LAST_NAME,
      LR.PATIENT_BIRTH_DATE,
      LR.PATIENT_MBI_OR_HICN ,
      LR.LAB_ORDER_NUMBER,
      LR.LAB_RESULT_NAME,
      LR.LAB_RESULT_OBSERVATION_DATE,
      LR.LAB_RESULT_OBSERVATION_TIME,
      LR.LAB_RESULT_VALUE,
      LR.LAB_RESULT_UNIT_OF_MEASURE,
      LR.LAB_RESULT_REFERENCE_RANGE,
      LR.LAB_RESULT_OBSERVE_STATUS,
      LR.LAB_RESULT_COMMENT ,
      LR.LAB_RESULT_TEST_NAME,
      LR.LAB_RESULT_TYPE ,
      LR.LAB_RESULT_LOINC_CODE,
      LR.LAB_RESULT_CPT_CODE,
      LR.LAB_RESULT_FILLER_ORDER_NUMBER,
      LR.LAB_RESULT_REQUESTED_DATE_TIME,
      LR.LAB_RESULT_ORDERED_DATE_TIME,
      LR.LAB_RESULT_OBSERVATION_REQUEST_TIME,
      LR.LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS,
      LR.LAB_RESULT_SPECIMEN_SOURCE,
      LR.LAB_RESULT_ABNORMAL_FLAG,
      LR.LAB_RESULT_NATURE_OF_ABNORMAL_TEST,
      LR.HASH
  FROM """ + labresultSourceTable + """ LR  INNER JOIN """ + dbName + "_" + targetMarket + "_inscope_patient" +
      """  IP ON LR.ENTITY_MEMBER_ID=IP.SOURCE_PATIENT_ID WHERE LR.LAB_RESULT_OBSERVATION_DATE <=SUBSTRING(IP.LASTELIGENDDATE,1,10)
      """

  def runNotebook(): String = {

    println("Running CDOS " +domainName.capitalize+ " Notebook")
    cdosBase.setLastExtractDT()

    println(s"CdosLabResult 1 : ${java.time.LocalDateTime.now}")

    val currentDT = cdosBase.getExtractDate_inscope(targetMarket, cdosBase.dbName)
    val currentDate = currentDT.replace("-", "").substring(2, 8)
    println("timetravel"+timeTravel)

    println(s"CdosLabResult 2 : ${java.time.LocalDateTime.now}")

    println("CURRENT_DATE: " + currentDate)
    println("NEW_CURRENT_DATE_TIMESTAMP: " + currentDT)

//temp for testing
    //val labresultSourceDF_new = sfCDCM.sql("select * from " + labresultSourceTable + "_temp")
    val labresultSourceDF_new = cdosBase.getDataframeFromSnowflake(labresultSourceQuery)

    println(s"CdosLabResult 3 : ${java.time.LocalDateTime.now}")

    cdosBase.dropTableifExists(labresultSourceTable)
    println("Dropped labresultSourceTable:" + labresultSourceTable)
    println("cdosBase.dropTableifExists(labresultSourceTable) completed")

    println(s"CdosLabResult 4 : ${java.time.LocalDateTime.now}")

    val labresultSourceDF = labresultSourceDF_new.withColumn("LAB_RESULT_COMMENT", when(col("lab_result_comment").is_null, col("lab_result_comment")).otherwise(regexp_replace(col("lab_result_comment"),lit("[\u0000-\u001F||\u00A0-\u00FF||\uFFF0-\uFFFF]"),lit("")))).withColumn("LAB_RESULT_UNIT_OF_MEASURE", when(col("LAB_RESULT_UNIT_OF_MEASURE").is_null, col("LAB_RESULT_UNIT_OF_MEASURE")).otherwise(regexp_replace(col("LAB_RESULT_UNIT_OF_MEASURE"),lit("[\u0000-\u001F||\u00A0-\u00FF||\uFFF0-\uFFFF]"),lit(""))))

    println("Starting: cdosBase.writeDataFrameToSnowflake")

    println(s"CdosLabResult 5 : ${java.time.LocalDateTime.now}")

    cdosBase.writeDataFrameToSnowflake(labresultSourceDF, labresultSourceTable, "overWrite")
    println("Completed: cdosBase.writeDataFrameToSnowflake")

    println(s"CdosLabResult 6 : ${java.time.LocalDateTime.now}")

    val labresultQuery = cdosBase.getDataframeFromSnowflake(labresultDF)

    println(s"CdosLabResult 7 : ${java.time.LocalDateTime.now}")

    val labResultDF_history_temp_str = cdosBase.defaultTable_insertion(targetMarket.toUpperCase, domainName,labresultQuery)

    val labResultDF_history_temp_before = labResultDF_history_temp_str
        .withColumn("PATIENT_BIRTH_DATE",(col("PATIENT_BIRTH_DATE")))
        .withColumn("LAB_RESULT_OBSERVATION_DATE",(col("LAB_RESULT_OBSERVATION_DATE")))

    println(s"CdosLabResult 8 : ${java.time.LocalDateTime.now}")

    val labResultDF_history_temp = labResultDF_history_temp_before.select("TENANT_SK", "SOURCE_SYSTEM_SK", "LAB_RESULT_SK", "SOURCE_SYSTEM", "CONTRACT", "PBP", "ENTITY_MEMBER_ID", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PATIENT_BIRTH_DATE", "PATIENT_MBI_OR_HICN", "LAB_ORDER_NUMBER", "LAB_RESULT_NAME", "LAB_RESULT_OBSERVATION_DATE", "LAB_RESULT_OBSERVATION_TIME", "LAB_RESULT_VALUE", "LAB_RESULT_UNIT_OF_MEASURE", "LAB_RESULT_REFERENCE_RANGE", "LAB_RESULT_OBSERVE_STATUS", "LAB_RESULT_COMMENT", "LAB_RESULT_TEST_NAME", "LAB_RESULT_TYPE", "LAB_RESULT_LOINC_CODE", "LAB_RESULT_CPT_CODE", "LAB_RESULT_FILLER_ORDER_NUMBER", "LAB_RESULT_REQUESTED_DATE_TIME", "LAB_RESULT_ORDERED_DATE_TIME", "LAB_RESULT_OBSERVATION_REQUEST_TIME", "LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS", "LAB_RESULT_SPECIMEN_SOURCE", "LAB_RESULT_ABNORMAL_FLAG", "LAB_RESULT_NATURE_OF_ABNORMAL_TEST", "HASH")

    val labResultDF_history_withouthash = labResultDF_history_temp.drop("HASH")

    val labResultDF_history_withouthash_withoutlabsk = labResultDF_history_withouthash.drop("LAB_RESULT_SK")

    def drop_hash():DataFrame = {
      labResultDF_history_temp.drop("HASH")
    }
    def hash_function(): DataFrame = {
        labResultDF_history_withouthash.withColumn("HASH", com.snowflake.snowpark.functions.hash(labResultDF_history_withouthash_withoutlabsk.col("*")))
    }
    val labResultDF_history_preincremental:DataFrame ={
      drop_hash()
      hash_function()
    }

    println(s"CdosLabResult 9 : ${java.time.LocalDateTime.now}")
    println(labResultDF_history_preincremental)

    var doesTableExistCount = sfCDCM.sql("select count(*) from INFORMATION_SCHEMA.TABLES where table_name = upper('" + labresultTable.split("\\.").last + "')").collect()(0)(0)

    println(s"CdosLabResult 10 : ${java.time.LocalDateTime.now}")
    println(doesTableExistCount)
    if (doesTableExistCount == 0)
    {
    labResultDF_history_preincremental.limit(0)
      .write.mode(com.snowflake.snowpark.SaveMode.Append)
      .saveAsTable(labresultTable);
    }

     sfCDCM.sql("alter table " + labresultTable + " set change_tracking = true;").collect()
    //Update snowflakeFormattedDate to be after table creation time for time travel
    var snowflakeFormattedDate = sfCDCM.sql("select current_timestamp() as SnowflakeFormattedDate").select("SnowflakeFormattedDate").collect().map(_.getTimestamp(0)).mkString("")

    var labDelta = sfCDCM.table(labresultTable)

    val allColumns = labResultDF_history_preincremental.schema.map(f => f.name -> labResultDF_history_preincremental(f.name)).toMap

    // update any records where the hashes do not match.



    labDelta
      .merge(labResultDF_history_preincremental,
      labDelta("TENANT_SK") === labResultDF_history_preincremental("TENANT_SK") and
      labDelta("SOURCE_SYSTEM_SK") === labResultDF_history_preincremental("SOURCE_SYSTEM_SK") and
      labDelta("ENTITY_MEMBER_ID") === labResultDF_history_preincremental("ENTITY_MEMBER_ID") and
      labDelta("LAB_ORDER_NUMBER") === labResultDF_history_preincremental("LAB_ORDER_NUMBER") and
      labDelta("LAB_RESULT_NAME") === labResultDF_history_preincremental("LAB_RESULT_NAME") and
      labDelta("LAB_RESULT_OBSERVATION_DATE") === labResultDF_history_preincremental("LAB_RESULT_OBSERVATION_DATE") and
      labDelta("LAB_RESULT_TEST_NAME") === labResultDF_history_preincremental("LAB_RESULT_TEST_NAME") and
      labDelta("LAB_RESULT_LOINC_CODE") === labResultDF_history_preincremental("LAB_RESULT_LOINC_CODE") and
      !(labDelta("HASH") === labResultDF_history_preincremental("HASH")))
      .whenMatched
      .update(allColumns)
      .collect()


    labDelta
      .merge(labResultDF_history_preincremental,
        labDelta("TENANT_SK") === labResultDF_history_preincremental("TENANT_SK") and
        labDelta("SOURCE_SYSTEM_SK") === labResultDF_history_preincremental("SOURCE_SYSTEM_SK") and
        labDelta("ENTITY_MEMBER_ID") === labResultDF_history_preincremental("ENTITY_MEMBER_ID") and
        labDelta("LAB_ORDER_NUMBER") === labResultDF_history_preincremental("LAB_ORDER_NUMBER") and
        labDelta("LAB_RESULT_NAME") === labResultDF_history_preincremental("LAB_RESULT_NAME") and
        labDelta("LAB_RESULT_OBSERVATION_DATE") === labResultDF_history_preincremental("LAB_RESULT_OBSERVATION_DATE") and labDelta("LAB_RESULT_TEST_NAME") === labResultDF_history_preincremental("LAB_RESULT_TEST_NAME") and
        labDelta("LAB_RESULT_LOINC_CODE") === labResultDF_history_preincremental("LAB_RESULT_LOINC_CODE "))
      .whenNotMatched
      .insert(allColumns)
      .collect()

    println(s"CdosLabResult 11 : ${java.time.LocalDateTime.now}")

    var incrementalEvents = sfCDCM.sql(
    """
    SELECT * FROM """ + labresultTable + """
    CHANGES(INFORMATION => DEFAULT)
    AT(TIMESTAMP => to_timestamp_ltz('""" + snowflakeFormattedDate + """'))
    WHERE METADATA$ACTION = 'INSERT'"""
    )

    val incrementalEvents3 = sfCDCM.sql(
      """
      SELECT * FROM """ + labresultTable +"""
    CHANGES(INFORMATION => DEFAULT)
    AT(TIMESTAMP => to_timestamp_ltz('""" + snowflakeFormattedDate + """'))
    WHERE METADATA$ACTION != 'INSERT'""" )

    var incrementalEvents2 = sfCDCM.sql(
    """
    SELECT * FROM """ + labresultTable + """
    CHANGES(INFORMATION => DEFAULT)
    AT(TIMESTAMP => to_timestamp_ltz('""" + snowflakeFormattedDate + """'))
    WHERE METADATA$ACTION = 'INSERT'
    AND METADATA$ISUPDATE = false"""
    )

    println(s"CdosLabResult 12 : ${java.time.LocalDateTime.now}")

    val labCurrentIncrementalDF =
      incrementalEvents
        .drop("LAB_RESULT_SK")
        .drop("DEL_IND")
        .drop("METADATA$ACTION")
        .drop("METADATA$ISUPDATE")
        .drop("METADATA$ROW_ID")
        .drop("HASH")

    println(s"CdosLabResult 13 : ${java.time.LocalDateTime.now}")

    var modifiednameDF = labCurrentIncrementalDF.limit(0)
    if ("SDM3".equals(cdosBase.sdm_extraction(cdosBase.targetMarket))) {
       modifiednameDF = labCurrentIncrementalDF.withColumn("PATIENT_NAME",when(col("MIDDLE_NAME").is_null,
                        com.snowflake.snowpark.functions.concat(col("LAST_NAME"),lit(","),col("FIRST_NAME"))).otherwise(concat(col("LAST_NAME"),lit(","), col("FIRST_NAME"),lit(" "), col("MIDDLE_NAME"))))
    }
    else {
      modifiednameDF = labCurrentIncrementalDF.withColumn("PATIENT_NAME",when(col("MIDDLE_NAME").is_null,com.snowflake.snowpark.functions.concat(col("LAST_NAME"),lit(","),col("FIRST_NAME"),lit(" "),lit(""))).otherwise(concat(col("LAST_NAME"),lit(","), col("FIRST_NAME"),lit(" "),col("MIDDLE_NAME"))))
    }

    println(s"CdosLabResult 14 : ${java.time.LocalDateTime.now}")

    val droppedcolnameDF = modifiednameDF.drop("FIRST_NAME","LAST_NAME","MIDDLE_NAME")

    println(s"CdosLabResult 15 : ${java.time.LocalDateTime.now}")


    val labResultDF_history_afterdefault = droppedcolnameDF.select(
      "SOURCE_SYSTEM",
      "TENANT_SK",
      "SOURCE_SYSTEM_SK",
      "CONTRACT",
      "PBP",
      "ENTITY_MEMBER_ID",
      "PATIENT_NAME",
      "PATIENT_BIRTH_DATE",
      "PATIENT_MBI_OR_HICN",
      "LAB_ORDER_NUMBER",
      "LAB_RESULT_NAME",
      "LAB_RESULT_OBSERVATION_DATE",
      "LAB_RESULT_OBSERVATION_TIME",
      "LAB_RESULT_VALUE",
      "LAB_RESULT_UNIT_OF_MEASURE",
      "LAB_RESULT_REFERENCE_RANGE",
      "LAB_RESULT_OBSERVE_STATUS",
      "LAB_RESULT_COMMENT",
      "LAB_RESULT_TEST_NAME",
      "LAB_RESULT_TYPE",
      "LAB_RESULT_LOINC_CODE",
      "LAB_RESULT_CPT_CODE",
      "LAB_RESULT_FILLER_ORDER_NUMBER",
      "LAB_RESULT_REQUESTED_DATE_TIME",
      "LAB_RESULT_ORDERED_DATE_TIME",
      "LAB_RESULT_OBSERVATION_REQUEST_TIME",
      "LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS",
      "LAB_RESULT_SPECIMEN_SOURCE",
      "LAB_RESULT_ABNORMAL_FLAG",
      "LAB_RESULT_NATURE_OF_ABNORMAL_TEST")

    println(s"CdosLabResult 16 : ${java.time.LocalDateTime.now}")

    val labResultDF_history = labResultDF_history_afterdefault.withColumn("LAB_ORDER_NUMBER_1", col("LAB_ORDER_NUMBER").cast(StringType)).drop("LAB_ORDER_NUMBER").rename("LAB_ORDER_NUMBER",col("LAB_ORDER_NUMBER_1"))

    println(s"CdosLabResult 17 : ${java.time.LocalDateTime.now}")

    val labResultDF_history_new = labResultDF_history.select(
      "SOURCE_SYSTEM",
      "TENANT_SK",
      "SOURCE_SYSTEM_SK",
      "CONTRACT",
      "PBP",
      "ENTITY_MEMBER_ID",
      "PATIENT_NAME",
      "PATIENT_BIRTH_DATE",
      "PATIENT_MBI_OR_HICN",
      "LAB_ORDER_NUMBER",
      "LAB_RESULT_NAME",
      "LAB_RESULT_OBSERVATION_DATE",
      "LAB_RESULT_OBSERVATION_TIME",
      "LAB_RESULT_VALUE",
      "LAB_RESULT_UNIT_OF_MEASURE",
      "LAB_RESULT_REFERENCE_RANGE",
      "LAB_RESULT_OBSERVE_STATUS",
      "LAB_RESULT_COMMENT",
      "LAB_RESULT_TEST_NAME",
      "LAB_RESULT_TYPE",
      "LAB_RESULT_LOINC_CODE",
      "LAB_RESULT_CPT_CODE",
      "LAB_RESULT_FILLER_ORDER_NUMBER",
      "LAB_RESULT_REQUESTED_DATE_TIME",
      "LAB_RESULT_ORDERED_DATE_TIME",
      "LAB_RESULT_OBSERVATION_REQUEST_TIME",
      "LAB_RESULT_OBSERVE_REQUEST_RESULT_STATUS",
      "LAB_RESULT_SPECIMEN_SOURCE",
      "LAB_RESULT_ABNORMAL_FLAG",
      "LAB_RESULT_NATURE_OF_ABNORMAL_TEST")

    println(s"CdosLabResult 18 : ${java.time.LocalDateTime.now}")


      val tenantList = tenant.split(",").map(_.trim)
      val columnOrder = labResultDF_history_new.schema.fields.map(_.name)

      val labResultDF_history_new_updated = if (tenantList.length > 1) {
        labResultDF_history_new.withColumn("TENANT_SK", lit(0)).withColumn("SOURCE_SYSTEM_SK", lit(0)).dropDuplicates()
      } else {
        labResultDF_history_new
      }

      val reorderedHistDf = labResultDF_history_new_updated.select(columnOrder.map(col))
      val labResultDF_Delta = reorderedHistDf.drop("SOURCE_SYSTEM_SK").drop("TENANT_SK")

      var lastExtractDate = ""
      try {
        lastExtractDate = cdosBase.getLastExtractDate()
      } catch {
        //    Value gets from setLastExtractDT
        case e: StringIndexOutOfBoundsException => lastExtractDate = cdosBase.lastExtractDT.replace("-", "").substring(2, 8)
      }
      println("lastExtractDate"+lastExtractDate)

      // FileName Extraction from Txt file
      val tmForFileName = cdosBase.filename_extraction(tenant,targetMarket.toUpperCase())

      val reportNamePrefix = tmForFileName + "_" + RunType + "_Lab_" + lastExtractDate + "_" + currentDate
      println(reportNamePrefix)
      // From Landing to Archive
      cdosBase.archiveExtracts(tmForFileName + "_Hist_Lab_")
      cdosBase.archiveExtracts(tmForFileName + "_" + RunType + "_Lab_" )
      // Creating new File Extract in Landing,Update the Last Extract Date same as Inscope Patient Extract Date
      cdosBase.generateExtractAndUpload(reportNamePrefix, labResultDF_Delta)

      val reportName = reportNamePrefix
      val valReportName = reportName + ".txt"
      cdosBase.loadExtractsHistoryTable(valReportName, reorderedHistDf)

      // Update the Previous_last_extract_dt for later usage.
      cdosBase.updatePreviousExtractDate(cdosBase.lastExtractDT,targetMarket,domainName)

      cdosBase.updateLastExtractDT_exceptinscope()
      val status = "Success"
      return domainName + status

  }
}
