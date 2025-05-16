// Databricks notebook source
// MAGIC %run ./CdosBase

// COMMAND ----------

// MAGIC %run ./CdosConstants

// COMMAND ----------

// MAGIC %run ./CdosBaseMedclaims

// COMMAND ----------

// MAGIC %run ./CdosConfigMapMedclaims

// COMMAND ----------

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.snowflake.snowpark.{DataFrame=>SpDataFrame}
import com.snowflake.snowpark._
import com.snowflake.snowpark.types.{StringType, StructField, StructType}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.Session
import java.time.ZoneOffset
class CdosMedclaimDetails(var notebookParams: scala.collection.mutable.Map[String, String], sfCDCM: Session) extends SparkSessionWrapper {

  val domainName = CdosConstants.cdosMedclaimDetailsDomainName
  val cdosBaseMedclaims = new CdosBaseMedclaims(notebookParams)
  val cdosBase = new CdosBase(notebookParams, sfCDCM)
  notebookParams += ("domainName" -> "MEDCLAIM_DETAILS")
  notebookParams += ("consumer" -> cdosBaseMedclaims.consumer)
  cdosBaseMedclaims.domainName = domainName
  cdosBase.domainName = domainName
  val CdosConfigMapMedclaims = new CdosConfigMapMedclaims(notebookParams, sfCDCM)
  var targetMarket = notebookParams("targetMarket").toLowerCase()
  var sourceSystem = notebookParams("sourceSystem")
  val lookbackPeriod = notebookParams("lookbackPeriod")
  val RunType = notebookParams("RunType")
  val databricksEnv = notebookParams("databricksEnv")
  val sfWarehouse = notebookParams("sfWarehouse")
  val consumer = notebookParams("consumer")
  val dbName = notebookParams("dbName")
  var featureName = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos", "").toLowerCase()
  var medDbName = "cdos" + featureName + "_snowpark"
  println("medDbName: "+medDbName)
  val medclaimDetailTbl = medDbName.concat(".").concat(targetMarket).concat("_").concat(domainName)
  println(medclaimDetailTbl)
  val medclaimDetailSourceTbl = medDbName.concat(".").concat(targetMarket).concat("_").concat(domainName).concat("_source")
  println(medclaimDetailSourceTbl)
  val medclaimVersionTbl = medDbName.concat(".").concat(targetMarket).concat("_").concat(domainName).concat("_version")
  println(medclaimVersionTbl)
  val sfEnv = notebookParams("snowflakeEnv")
  val sfDatabase = "OCDP_"+sfEnv+"_CDCM_DB"
  var targetQuery: String = null
  var timeTravel=""
  var timeTravelDate = ""
  val useSparkLastExtractForTimeTravel=notebookParams("useSparkLastExtractForTimeTravel")
  if (useSparkLastExtractForTimeTravel == "true")
    {timeTravel = cdosBase.getTimeTravel(domainName, targetMarket)}
  timeTravelDate = cdosBase.getDateForLookbackPeriod(timeTravel)
  println("timeTravelDate: " + timeTravelDate)
  
  var tenant = notebookParams("tenant")
  val TENANT_NUMBER = "(" + tenant + ")"
  var source = if (sourceSystem != "All") " AND cld.source_system_sk in (" + sourceSystem + ")" else "";
  println(cdosBaseMedclaims.targetMarket)
  println("")
  println(CdosConfigMapMedclaims.sdm_type)
  println("")
  println(CdosConfigMapMedclaims.eid)
  println("")
  if ("SDM2".equals(CdosConfigMapMedclaims.sdm_type)) {
    targetQuery =
      s"""
      SELECT  src.source_system_abbr AS SOURCE_SYSTEM,
           CASE WHEN  cld.SOURCE_SYSTEM_SK = '610' THEN cld.claim_number
           ELSE TRIM(concat(cld.source_system_sk,'-',cld.claim_number)) END AS CLAIM_NUMBER,
           cld.claim_line AS SERVICE_LINE,
           CAST(CASE WHEN cld.ub_rev_code IS NULL THEN NULL
           WHEN TRIM(cld.ub_rev_code)='UNK' THEN cld.ub_rev_code ELSE RIGHT(('000'||cld.ub_rev_code),4) END AS VARCHAR(48)) AS REVENUE_CODE,
           cld.cms_pos AS FACILITY_TYPE_CODE,
           cld.procedure_code AS PROCEDURE_CODE,
           cld.mod_1 AS MODIFIER_1,
           cld.mod_2 AS MODIFIER_2,
           cld.mod_3 AS MODIFIER_3,
           cld.mod_4 AS MODIFIER_4,
           CASE WHEN (cld.er_flg = 'Y' OR cld.er_flg = 'N')
           THEN cld.er_flg ELSE NULL END
           AS EMERGENCY_FLAG,
           cld.ndc_code AS NDC_CODE,
           CASE WHEN (cld.hospital_flg = 'Y' OR  cld.hospital_flg = 'N')
           THEN cld.hospital_flg
           ELSE NULL END AS HOSPITAL_RELATED_FLAG,
           CASE WHEN (cld.outside_lab_flg = 'Y' OR cld.outside_lab_flg = 'N')
           THEN cld.outside_lab_flg
           ELSE NULL END AS OUTSIDE_LABS_FLAG,
           CAST(CASE WHEN TRIM(cld.statistical_ind)=1 THEN 'Y' ELSE 'N' END AS VARCHAR(1)) AS CAPPED_INDICATOR,
           nvl(cld.chg_amt,'0.00') AS BILLED_AMOUNT,
           cld.allowed_amt AS ALLOWED_AMOUNT,
           cld.pmt_amt AS PAID_AMOUNT,
           nvl(cld.approved_units,'0') as UNIT_COUNT,
           CASE WHEN cld.serv_from_dt < '1900-01-01T00:00:00.000+00:00' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE cld.serv_from_dt END AS SERVICE_FROM_DATE,
           CASE WHEN cld.serv_thru_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
                ELSE cld.serv_thru_dt END AS SERVICE_TO_DATE,
           CASE WHEN cld.billed_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
                ELSE cld.billed_dt END AS BILLED_DATE,
           CASE WHEN cld.processed_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
                ELSE cld.processed_dt END AS PROCESSED_DATE,
           CASE WHEN cld.paid_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
                ELSE cld.paid_dt END AS PAID_DATE,
           cld.del_ind,
           clh.source_patient_id,
           cld.claim_detail_sk,
           clh.serv_from_dt,
           cld.tenant_sk,
           cld.source_system_sk,
           cld.claim_header_sk,
           HASH(SOURCE_SYSTEM,cld.CLAIM_NUMBER,SERVICE_LINE,REVENUE_CODE,FACILITY_TYPE_CODE,cld.PROCEDURE_CODE,MODIFIER_1,MODIFIER_2,
    MODIFIER_3,MODIFIER_4,EMERGENCY_FLAG,NDC_CODE,HOSPITAL_RELATED_FLAG,OUTSIDE_LABS_FLAG,CAPPED_INDICATOR,BILLED_AMOUNT,
    ALLOWED_AMOUNT,PAID_AMOUNT,UNIT_COUNT,SERVICE_FROM_DATE,SERVICE_TO_DATE,BILLED_DATE,PROCESSED_DATE,PAID_DATE,cld.del_ind) as HASH
      FROM """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosMedclaimDetailsMaterialized + timeTravel + """ cld
      INNER JOIN """ + cdosBaseMedclaims.sfSchema + """.source_system """+timeTravel+"""src
      ON cld.source_system_sk = src.source_system_sk
      INNER JOIN """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosMedclaimHeaderMaterialized + timeTravel + """ clh
      ON cld.claim_header_sk = clh.claim_header_sk AND clh.serv_from_dt >= to_timestamp_ntz(DATEADD(month, -""" + lookbackPeriod + """,
      date_trunc(month, """ + timeTravelDate + """))) AND IFNULL(clh.final_status_flg, 'Y') = 'Y'
      LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosProcedureClaimMaterialized + timeTravel+""" clpp
      ON cld.procedure_code_sk = clpp.claim_procedure_sk AND clpp.DEL_IND = 0
      WHERE  cld.tenant_sk IN """ + TENANT_NUMBER + """
      AND src.DEL_IND = 0
      """ + source + """
      AND cld.serv_from_dt is not null
      AND (cld.aud_upd_dt >= to_timestamp_ntz(dateadd(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))) OR clpp.aud_upd_dt >= to_timestamp_ntz(dateadd(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))) OR clh.aud_upd_dt >= to_timestamp_ntz(dateadd(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))))
      """}
  else if ("SDM3".equals(CdosConfigMapMedclaims.sdm_type)) {
    targetQuery =
      s"""
    SELECT  src.source_system_abbr AS SOURCE_SYSTEM,
         CASE WHEN  cld.SOURCE_SYSTEM_SK = '610' THEN cld.claim_number
         ELSE TRIM(concat(cld.source_system_sk,'-',cld.claim_number)) END AS CLAIM_NUMBER,
         cld.claim_line AS SERVICE_LINE,
         CAST(CASE WHEN cld.ub_rev_code IS NULL THEN NULL
         WHEN TRIM(cld.ub_rev_code)='UNK' THEN cld.ub_rev_code ELSE RIGHT(('000'||cld.ub_rev_code),4) END AS VARCHAR(48)) AS REVENUE_CODE,
         cld.cms_pos AS FACILITY_TYPE_CODE,
         cld.procedure_code AS PROCEDURE_CODE,
         cld.mod_1 AS MODIFIER_1,
         cld.mod_2 AS MODIFIER_2,
         cld.mod_3 AS MODIFIER_3,
         cld.mod_4 AS MODIFIER_4,
         CASE WHEN (cld.er_flg = 'Y' OR cld.er_flg = 'N')
         THEN cld.er_flg ELSE NULL END
         AS EMERGENCY_FLAG,
         cld.ndc_code AS NDC_CODE,
         CASE WHEN (cld.hospital_flg = 'Y' OR  cld.hospital_flg = 'N')
         THEN cld.hospital_flg
         ELSE NULL END AS HOSPITAL_RELATED_FLAG,
         CASE WHEN (cld.outside_lab_flg = 'Y' OR cld.outside_lab_flg = 'N')
         THEN cld.outside_lab_flg
         ELSE NULL END AS OUTSIDE_LABS_FLAG,
         CAST(CASE WHEN TRIM(cld.statistical_ind)=1 THEN 'Y' ELSE 'N' END AS VARCHAR(1)) AS CAPPED_INDICATOR,
         CASE WHEN LENGTH(TRIM(cld.chg_amt)) > 13 THEN NULL ELSE TRIM(cld.chg_amt) END AS BILLED_AMOUNT,
         CASE WHEN LENGTH(TRIM(cld.allowed_amt)) > 13 THEN NULL ELSE TRIM(cld.allowed_amt) END AS ALLOWED_AMOUNT,
         CASE WHEN LENGTH(TRIM(cld.pmt_amt)) > 13 THEN NULL ELSE TRIM(cld.pmt_amt) END AS PAID_AMOUNT,
         nvl(cld.approved_units,'0') as UNIT_COUNT,
         CASE WHEN cld.serv_from_dt < '1900-01-01T00:00:00.000+00:00' THEN '1900-01-01T00:00:00.000+00:00'
              ELSE cld.serv_from_dt END AS SERVICE_FROM_DATE,
         CASE WHEN cld.serv_thru_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
              ELSE cld.serv_thru_dt END AS SERVICE_TO_DATE,
         CASE WHEN cld.billed_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
              ELSE cld.billed_dt END AS BILLED_DATE,
         CASE WHEN cld.processed_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
              ELSE cld.processed_dt END AS PROCESSED_DATE,
         CASE WHEN cld.paid_dt < '1900-01-01T00:00:00.000+00:00' THEN NULL
              ELSE cld.paid_dt END AS PAID_DATE,
         cld.del_ind,
         clh.source_patient_id,
         cld.claim_detail_sk,
         clh.serv_from_dt,
         cld.tenant_sk,
         cld.source_system_sk,
         cld.claim_header_sk,
         HASH(SOURCE_SYSTEM,cld.CLAIM_NUMBER,SERVICE_LINE,REVENUE_CODE,FACILITY_TYPE_CODE,cld.PROCEDURE_CODE,MODIFIER_1,MODIFIER_2,
  MODIFIER_3,MODIFIER_4,EMERGENCY_FLAG,NDC_CODE,HOSPITAL_RELATED_FLAG,OUTSIDE_LABS_FLAG,CAPPED_INDICATOR,BILLED_AMOUNT,
  ALLOWED_AMOUNT,PAID_AMOUNT,UNIT_COUNT,SERVICE_FROM_DATE,SERVICE_TO_DATE,BILLED_DATE,PROCESSED_DATE,PAID_DATE,cld.del_ind) as HASH
    FROM """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosMedclaimDetailsMaterialized + timeTravel+""" cld
         INNER JOIN """ + cdosBaseMedclaims.sfSchema + """.source_system """+timeTravel+"""src
         ON cld.source_system_sk = src.source_system_sk
         INNER JOIN """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosMedclaimHeaderMaterialized +timeTravel+""" clh
         ON cld.claim_header_sk = clh.claim_header_sk AND clh.serv_from_dt >= to_timestamp_ntz(DATEADD(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))) AND IFNULL(clh.final_status_flg, 'Y') = 'Y'
         LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.""" + CdosConstants.cdosProcedureClaimMaterialized + timeTravel+""" clpp
         ON cld.procedure_code_sk = clpp.claim_procedure_sk AND clpp.DEL_IND = 0
         WHERE  cld.tenant_sk IN """ + TENANT_NUMBER + """
         AND src.DEL_IND = 0
         """ + source + """
         AND cld.serv_from_dt is not null         
         AND (cld.aud_upd_dt >= to_timestamp_ntz(dateadd(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))) OR clpp.aud_upd_dt >= to_timestamp_ntz(dateadd(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))) OR clh.aud_upd_dt >= to_timestamp_ntz(dateadd(month, -""" + lookbackPeriod + """, date_trunc(month, """ + timeTravelDate + """))))
         """
  }
  else ""

  cdosBaseMedclaims.query_writer(targetMarket.toLowerCase, domainName,cdosBaseMedclaims.consumer, targetQuery)

  var medclaimDetailDFQuery =
    """
  SELECT
  cld.SOURCE_SYSTEM,
  cld.CLAIM_NUMBER,
  cld.SERVICE_LINE,
  cld.REVENUE_CODE,
  cld.FACILITY_TYPE_CODE,
  cld.PROCEDURE_CODE,
  cld.MODIFIER_1,
  cld.MODIFIER_2,
  cld.MODIFIER_3,
  cld.MODIFIER_4,
  cld.EMERGENCY_FLAG,
  cld.NDC_CODE,
  cld.HOSPITAL_RELATED_FLAG,
  cld.OUTSIDE_LABS_FLAG,
  cld.CAPPED_INDICATOR,
  cld.PAID_AMOUNT,
  cld.BILLED_AMOUNT,
  cld.ALLOWED_AMOUNT,
  cld.UNIT_COUNT,
  cld.SERVICE_FROM_DATE,
  cld.SERVICE_TO_DATE,
  cld.BILLED_DATE,
  cld.PROCESSED_DATE,
  cld.PAID_DATE,
  cld.CLAIM_DETAIL_SK,
  cld.TENANT_SK,
  cld.SOURCE_SYSTEM_SK,
  cld.CLAIM_HEADER_SK,
  cld.DEL_IND,
  cld.HASH
    """

  def runNotebook(): String = {
    println("Running CDOS " +domainName.capitalize+ " Notebook")
    cdosBase.setLastExtractDT()

    println(s"CdosMedclaimDetails 1 : ${java.time.LocalDateTime.now}")

    println("")
    cdosBaseMedclaims.currentDT = cdosBase.getExtractDate_inscope(cdosBaseMedclaims.targetMarket, dbName)
    println("")

    println(s"CdosMedclaimDetails 2 : ${java.time.LocalDateTime.now}")

    cdosBaseMedclaims.currentDate = cdosBaseMedclaims.currentDT.toString.replace("-", "").substring(2, 8)
    val currentDTNew = java.time.ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).toString()

    println(s"CdosMedclaimDetails 3 : ${java.time.LocalDateTime.now}")

    //Read patientQuery from Snowflake into DataFrame
    var sourceDF = cdosBaseMedclaims.getDataframeFromSnowflake(targetQuery, cdosBaseMedclaims.sfSchema)
    //Drop and Write to the Table
    cdosBaseMedclaims.dropTableifExists(medclaimDetailSourceTbl)
    println("Dropped labresultSourceTable:" + labresultSourceTable)
    println("cdosBase.dropTableifExists(labresultSourceTable) completed")
    cdosBaseMedclaims.writeDataFrameToSnowflake(sourceDF, medclaimDetailSourceTbl, "overWrite")
    


    println(s"CdosMedclaimDetails 4 : ${java.time.LocalDateTime.now}")
    
    val inscope_patient_sp_table = dbName + "_" + targetMarket + "_inscope_patient"
    val inscope_patient_sp_table_query = "select * from "+inscope_patient_sp_table
    var inscopeDf_sp= sfCDCM.sql(inscope_patient_sp_table_query)
     
    
    val temp_inscope_patient_table = medDbName+"."+"temp_inscope_patient_sp_"+targetMarket
    println("temp_inscope_patient_table: "+temp_inscope_patient_table)
    println("temp_inscope_patient_table end")
    
    cdosBaseMedclaims.dropTableifExists(temp_inscope_patient_table)
    cdosBaseMedclaims.writeDataFrameToSnowflake(inscopeDf_sp, temp_inscope_patient_table, "overWrite")
    println("inscopeDf_sp wrote to Snowflake")
    

    medclaimDetailDFQuery= medclaimDetailDFQuery+"""FROM """ + medDbName + "." + targetMarket + "_" + domainName + "_source" +
      """ cld
    INNER JOIN """ + temp_inscope_patient_table +
      """ isp
    ON cld.source_patient_id = isp.source_patient_id
    AND cld.tenant_sk = isp.tenant_sk
    AND isp.del_ind = 0
    AND isp.projectabbr = 'CDOS'
    AND cld.serv_from_dt <= isp.lasteligenddate"""

    var test_query = "select count(*) from "+temp_inscope_patient_table
    var temp_incope_count = spark.sql(test_query)
    println("temp_incope_count: "+temp_incope_count)

       
    val medclaimDetailDF = sfCDCM.sql(medclaimDetailDFQuery)

    println(s"CdosMedclaimDetails 5 : ${java.time.LocalDateTime.now}")

    //Default table function
    val medclaimDetailDF_history_temp = cdosBaseMedclaims.defaultTable_insertion(targetMarket.toUpperCase, domainName, medclaimDetailDF)
    val medclaimDetailDF_history_withouthash = medclaimDetailDF_history_temp.drop("HASH")
    val medclaimDetailDF_history_withouthash_withoutmedclaim_header_sk = medclaimDetailDF_history_withouthash.drop("CLAIM_HEADER_SK")

    println(s"CdosMedclaimDetails 6 : ${java.time.LocalDateTime.now}")

    def drop_hash():DataFrame = {
      medclaimDetailDF_history_temp.drop("HASH")
    }

    def hash_function():DataFrame = {
      medclaimDetailDF_history_withouthash.withColumn("HASH", hash(medclaimDetailDF_history_withouthash_withoutmedclaim_header_sk.columns.map(col):_*))
    }

    def medclaimDetailDF_history_preincremental():DataFrame = {
      drop_hash()
      hash_function()
    }

    println(s"CdosMedclaimDetails 7 : ${java.time.LocalDateTime.now}")

    // Create table if not exists
    var doesTableExistCount = sfCDCM.sql("select count(*) from INFORMATION_SCHEMA.TABLES where table_name = upper('" + medclaimDetailTbl.split("\\.").last + "')").collect()(0)(0)
    
    if (doesTableExistCount == 0) {
      medclaimDetailDF_history_preincremental.limit(0)
        .write.mode(com.snowflake.snowpark.SaveMode.Append)
        .saveAsTable(medclaimDetailTbl)
    }

    // Enable change tracking
    sfCDCM.sql("alter table " + medclaimDetailTbl + " set change_tracking = true;").collect()

    println(s"CdosMedclaimDetails 8 : ${java.time.LocalDateTime.now}")

    // Get current timestamp for time travel
    val snowflakeFormattedDate = sfCDCM.sql("select current_timestamp() as SnowflakeFormattedDate").select("SnowflakeFormattedDate").collect().map(_.getTimestamp(0)).mkString("")

    val targetTable = sfCDCM.table(medclaimDetailTbl)
    val allColumns = medclaimDetailDF_history_preincremental.schema.map(f => f.name -> medclaimDetailDF_history_preincremental(f.name)).toMap

    // Merge operation for updates
    targetTable
      .merge(medclaimDetailDF_history_preincremental,
        targetTable("CLAIM_NUMBER") === medclaimDetailDF_history_preincremental("CLAIM_NUMBER") &&
        targetTable("SERVICE_LINE") === medclaimDetailDF_history_preincremental("SERVICE_LINE") &&
        !(targetTable("HASH") === medclaimDetailDF_history_preincremental("HASH")))
      .whenMatched
      .update(allColumns)
      .collect()

    // Merge operation for inserts
    targetTable
      .merge(medclaimDetailDF_history_preincremental,
        targetTable("CLAIM_NUMBER") === medclaimDetailDF_history_preincremental("CLAIM_NUMBER") &&
        targetTable("SERVICE_LINE") === medclaimDetailDF_history_preincremental("SERVICE_LINE"))
      .whenNotMatched
      .insert(allColumns)
      .collect()

    println(s"CdosMedclaimDetails 9 : ${java.time.LocalDateTime.now}")

    // Get latest version
    val detailsLatestVersion = sfCDCM.sql(s"""SELECT MAX(CHANGE_TRACKING_CURRENT_VERSION()) FROM """ + medclaimDetailTbl).collect()(0)(0).asInstanceOf[Long]

    // Create version table
    val versionDF = sfCDCM.createDataFrame(Seq(Row(detailsLatestVersion)), StructType(Seq(StructField("Latest_Detail_Version", LongType, false))))
    cdosBaseMedclaims.writeDataFrameToSnowflake(versionDF, medclaimVersionTbl, "overWrite")

    println(s"CdosMedclaimDetails 10 : ${java.time.LocalDateTime.now}")

    val status = "Success"
    return domainName + status
  }

}
