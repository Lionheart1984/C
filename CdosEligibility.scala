// Databricks notebook source
// MAGIC %run ./CdosBase

// COMMAND ----------

// MAGIC %run ./CdosConstants

// COMMAND ----------

// MAGIC %run ./CdosConfigMap

// COMMAND ----------

import com.snowflake.snowpark.{DataFrame=>SpDataFrame}
import scala.language.{postfixOps, reflectiveCalls}
import scala.util.control.NonFatal
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.functions._

class CdosEligibility (var notebookParams: scala.collection.mutable.Map[String, String], sfCDCM: Session) extends SparkSessionWrapper {


  // Variable declarations
  val domainName = CdosConstants.cdosEligibilityDomainName
  var cdosBase = new CdosBase(notebookParams, sfCDCM)
  notebookParams += ("domainName" -> "ELIGIBILITY")
  notebookParams += ("consumer" -> cdosBase.consumer)
  cdosBase.domainName = domainName
  var cdosConfigMap = new CdosConfigMap(notebookParams, sfCDCM)
  var targetMarket = notebookParams("targetMarket").toLowerCase()
  var sourceSystem = notebookParams("sourceSystem")
  var lookbackPeriod = notebookParams("lookbackPeriod")
  var dbName = notebookParams("dbName")
  var databricksEnv = notebookParams("databricksEnv")
  var targetQuery: String = null
  val RunType = notebookParams("RunType")
  var timeTravel=""
  var timeTravelDate = ""
  val useSparkLastExtractForTimeTravel=notebookParams("useSparkLastExtractForTimeTravel")
  if (useSparkLastExtractForTimeTravel == "true")
  {timeTravel = cdosBase.getTimeTravel(domainName, targetMarket)}
  timeTravelDate = cdosBase.getDateForLookbackPeriod(timeTravel)
  println("timeTravelDate: " + timeTravelDate)

  var Record_Start_Date = notebookParams("Record_Start_Date")
  val eligibiltyTable = cdosBase.dbName.concat("_").concat(cdosBase.targetMarket).concat("_").concat(domainName)
  val eligibiltySourceTable = cdosBase.dbName.concat("_").concat(cdosBase.targetMarket).concat("_").concat(domainName).concat("_source")
  if (cdosBase.targetMarket.matches("ct")) {
    Record_Start_Date = notebookParams("Record_Start_Date");
    if (!cdosBase.validateDate(Record_Start_Date)) {
      throw new Exception("Invalid date passed")
    }
    Record_Start_Date = Record_Start_Date + " 00:00:00.000"
  }

  var tenant = notebookParams("tenant")
  val TENANT_NUMBER = "(" + tenant + ")"
  var source = if (sourceSystem != "All") " AND e.SOURCE_SYSTEM_SK in (" + sourceSystem + ")" else ""

  val trunc_lob_string_in =
    """, date_trunc(month, """ + timeTravelDate + """)))) and UPPER(e.LINE_OF_BUSINESS) in ("""

  val trunc_lob_string_ex =
    """) and UPPER(e.LINE_OF_BUSINESS) not in ("""


  val join_string = """ join """

  val pod_xwalk_string =
    """.POD_ID_XWALK """ + timeTravel + """ pxwalk on pxwalk.SOURCE_POD_ID = e.pod_id INNER JOIN """

  val provider_group_sk_string =
    """.Provider_Group """ + timeTravel + """ PG  on e.PROVIDER_GROUP_SK=PG.PROVIDER_GROUP_SK INNER JOIN """

  val source_system_string =
    """.SOURCE_SYSTEM """ + timeTravel + """ SS  ON e.SOURCE_SYSTEM_SK=SS.SOURCE_SYSTEM_SK LEFT JOIN """

  val reference_xwalk_string =
    """.OPTUMCARE_REFERENCE.OCDP_BUSINESS_TYPE_CROSSWALK """ + timeTravel + """ BTXWALK  on (upper(e.BUSINESS_TYPE)=upper(BTXWALK.STANDARD_VALUE) and BTXWALK.ACTIVE_FLAG=1 and BTXWALK.SOURCE='CDOS') LEFT JOIN """

  val tenant_sk_string =
    """.TENANT """ + timeTravel + """ t  ON e.TENANT_SK=t.TENANT_SK """

  val benefitplan_string = """ and IFNULL(e.BENEFIT_PLAN_TERM_DT,date('9999-12-31')) >=to_timestamp_ntz((DATEADD(MONTH , -"""

  val record_end_dt_string = """, date_trunc(month, """ + timeTravelDate + """)))) and IFNULL(e.RECORD_END_DT,date('9999-12-31')) >=to_timestamp_ntz((DATEADD(MONTH , -"""

  val all_del_ind_string = """ where e.DEL_IND = 0 and PG.DEL_IND = 0 and SS.DEL_IND = 0 and pxwalk.DEL_IND = 0 """

  val param_tenant_sk_string = """and e.TENANT_SK IN"""



  val sqlEligibilityTableDF_history_withoutdefault: String =
    """ Select
                e.SOURCE_SYSTEM as Source_System,
                e.SOURCE_SYSTEM_SK,
                e.TENANT_SK,
                e.ENTITY_MEMBER_ID as Entity_Member_Id,
                e.HP_MEMBER_ID,
                e.PROVIDER_ID as Provider_Id,
                e.PROVIDER_GROUP_ID as Provider_Group_Id,
                e.LOCATION_ID as Location_Id,
                cast(e.ELIGIBILITY_POD_ID as varchar) as Eligibility_Pod_Id,
                e.RECORD_START_DT as Record_Start_Dt,
                e.RECORD_END_DT as Record_End_Dt,
                e.HEALTH_PLAN as Health_Plan,
                e.BENEFIT_PLAN_KEY,
                e.BENEFIT_PLAN_DESC as Benefit_Plan_Desc,
                e.CONTRACT as Contract,
                e.PBP,
                e.BUSINESS_TYPE as Business_Type,
                e.BUSINESS_LINE as Business_Line,
                e.BENEFIT_PLAN_EFF_DATE as Benefit_Plan_Eff_Date,
                e.BENEFIT_PLAN_TERM_DATE as Benefit_Plan_Term_Date
        FROM """ + eligibiltySourceTable + """ e
  INNER JOIN """ + cdosBase.dbName + "_" + cdosBase.targetMarket + "_inscope_patient " + """ isp
        ON e.ENTITY_MEMBER_ID = isp.SOURCE_PATIENT_ID AND isp.Tenant_SK = e.TENANT_SK and isp.ProjectAbbr = 'CDOS'
  """

  val providertinXclusion = cdosBase.filterReferenceList_Exclusion(cdosBase.targetMarket, domainName, "PROVIDER_GROUP_TIN", cdosBase.consumer)
  println("providertinXclusionlist:" + providertinXclusion)
  val lob_in: String = cdosBase.filterReferenceList_Inclusion(cdosBase.targetMarket, domainName, "LINE_OF_BUSINESS", cdosBase.consumer)
  println("LINE_OF_BUSINESS_INCLUSION:" + lob_in)
  val lob_ex: String = cdosBase.filterReferenceList_Exclusion(cdosBase.targetMarket, domainName, "LINE_OF_BUSINESS", cdosBase.consumer)
  println("LINE_OF_BUSINESS_INCLUSION:" + lob_in)
  val healthplan_in = cdosBase.filterReferenceList_Inclusion(targetMarket, domainName, "HEALTH_PLAN_STANDARD_VALUE", cdosBase.consumer)
  println("HEALTH_PLAN_STANDARD_VALUE_INCLUSION:" + healthplan_in)
  val healthplan_ex = cdosBase.filterReferenceList_Exclusion(targetMarket, domainName, "HEALTH_PLAN_STANDARD_VALUE", cdosBase.consumer)
  println("HEALTH_PLAN_STANDARD_VALUE_EXCLUSION:" + healthplan_ex)

  println(cdosBase.targetMarket)
  println("")
  println(cdosConfigMap.sdm_type)
  println("")
  println(cdosConfigMap.eid)
  println("")
  if ("SDM2".equals(cdosConfigMap.sdm_type)) {
    targetQuery =
      f"""Select * exclude(elig_rank) from (Select distinct SS.SOURCE_SYSTEM_ABBR AS SOURCE_SYSTEM,
                e.SOURCE_SYSTEM_SK as SOURCE_SYSTEM_SK,
                e.TENANT_SK as TENANT_SK,
                e.SOURCE_PATIENT_ID AS ENTITY_MEMBER_ID,
                e.HEALTH_PLAN_CARD_ID AS HP_MEMBER_ID,
                IFF(nvl(e.Enrollment_PCP_ID,'0') = '-1', '0', nvl(e.Enrollment_PCP_ID,'0')) AS PROVIDER_ID,
                CASE WHEN nvl(e.PROVIDER_GROUP_ID,'0') = '-1' THEN '0'
                     WHEN e.PROVIDER_GROUP_ID IS NULL THEN '0'
                     WHEN TRUE THEN NVL(PGXWALK.REVISED_PROVIDER_GROUP_ID,'0') END AS PROVIDER_GROUP_ID,
                """+ cdosConfigMap.configuration_map("MTNWEST_LOCATION_ID_ELIGIBILITY_CONDITION") + """ AS LOCATION_ID,
                """+ cdosConfigMap.configuration_map("MTNWEST_MIDWEST_CALI_REVISED_POD_ID_ELIGIBILITY_CONDITION") +""" AS ELIGIBILITY_POD_ID,
                CASE WHEN e.RECORD_START_DT < '1900-01-01' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE e.RECORD_START_DT END AS RECORD_START_DT,
                CASE WHEN e.RECORD_END_DT < '1900-01-01' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE e.RECORD_END_DT END AS RECORD_END_DT,
                e.HEALTH_PLAN_STANDARD_VALUE AS HEALTH_PLAN,
                """+ cdosConfigMap.configuration_map("MTNWEST_MIDWEST_CALI_BENEFIT_PLAN_KEY_ELIGIBILITY_CONDITION")+""" AS BENEFIT_PLAN_KEY,
                e.BENEFIT_PLAN_DESC AS BENEFIT_PLAN_DESC,
                e.CONTRACT_NUMBER AS Contract,
                e.PLAN_BENEFIT_PACKAGE AS PBP,
                upper(BTXWALK.STANDARD_VALUE) AS BUSINESS_TYPE,
                e.LINE_OF_BUSINESS AS BUSINESS_LINE,
                CASE WHEN e.BENEFIT_PLAN_EFFECTIVE_DT < '1900-01-01' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE e.BENEFIT_PLAN_EFFECTIVE_DT END AS BENEFIT_PLAN_EFF_DATE,
                CASE WHEN e.BENEFIT_PLAN_TERM_DT < '1900-01-01' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE e.BENEFIT_PLAN_TERM_DT END AS BENEFIT_PLAN_TERM_DATE,
                ROW_NUMBER() OVER (PARTITION BY e.tenant_sk, e.source_patient_id, e.record_start_dt ORDER BY
    case when e.primary_coverage_flg = 'Y' then 1
    else 0 end DESC, e.aud_upd_dt desc, e.eligibility_sk desc) elig_rank
                from """ + cdosBase.sfSchema + """."""+ CdosConstants.cdosEligibilityMaterialized + timeTravel  + """ e LEFT JOIN """ +
        cdosBase.sfSchema + """.PROVIDER_GROUP_ID_XWALK """ + timeTravel + """ PGXWALK ON e.PROVIDER_GROUP_SK = PGXWALK.PROVIDER_GROUP_SK
        AND PGXWALK.DEL_IND = 0 AND e.PROVIDER_GROUP_SK NOT IN ('0','-1') """ +
        join_string + cdosBase.sfSchema + pod_xwalk_string +
        cdosConfigMap.configuration_map("MTNWEST_MIDWEST_CALI_JOIN_REVISED_POD_ID_ELIGIBILITY_CONDITION")  +
        cdosBase.sfSchema + provider_group_sk_string + cdosBase.sfSchema + source_system_string +
        cdosBase.sfReferenceDatabase + reference_xwalk_string + cdosBase.sfSchema + tenant_sk_string +
        all_del_ind_string + param_tenant_sk_string + TENANT_NUMBER +
        cdosConfigMap.configuration_map("IN_ELIGIBILITY_DEL_IND_CONDITION") +
        benefitplan_string + cdosBase.lookbackPeriod +
        record_end_dt_string + cdosBase.lookbackPeriod +
        trunc_lob_string_in + lob_in + trunc_lob_string_ex + lob_ex + """)""" +
        cdosConfigMap.configuration_map("HEALTH_PLAN_ELIGIBILITY_CONDITION") +
        source + """) where elig_rank = 1"""
  }
  else if ("SDM3".equals(cdosConfigMap.sdm_type)) {

    targetQuery =
      f"""Select * exclude(elig_rank) from (Select distinct SS.SOURCE_SYSTEM_ABBR AS SOURCE_SYSTEM,
              e.SOURCE_SYSTEM_SK as SOURCE_SYSTEM_SK,
              e.TENANT_SK as TENANT_SK,
              e.SOURCE_PATIENT_ID AS ENTITY_MEMBER_ID,
              CASE WHEN LENGTH(e.HEALTH_PLAN_CARD_ID) > 20 THEN NULL ELSE e.HEALTH_PLAN_CARD_ID END AS HP_MEMBER_ID,

              """+ cdosConfigMap.configuration_map("CALI_PROVIDER_ID_ELIGIBILITY_CONDITION") +""" AS PROVIDER_ID,

              CASE WHEN nvl(e.PROVIDER_GROUP_ID,'0') = '-1' THEN '0'
                   WHEN e.PROVIDER_GROUP_ID IS NULL THEN '0'
                   WHEN TRUE THEN NVL(PGXWALK.REVISED_PROVIDER_GROUP_ID,'0') END AS PROVIDER_GROUP_ID,
              """+ cdosConfigMap.configuration_map("MTNWEST_LOCATION_ID_ELIGIBILITY_CONDITION") +""" AS LOCATION_ID,
              """+ cdosConfigMap.configuration_map("MTNWEST_MIDWEST_CALI_REVISED_POD_ID_ELIGIBILITY_CONDITION") +""" AS ELIGIBILITY_POD_ID,
              """ + cdosConfigMap.configuration_map("SDM3_RECORD_START_DATE_ELIGIBILITY_CONDITION") +""" AS RECORD_START_DT,
              CASE WHEN e.RECORD_END_DT < '1900-01-01' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE e.RECORD_END_DT END AS RECORD_END_DT,
              substring(TRIM(e.HEALTH_PLAN_STANDARD_VALUE),1,15) AS HEALTH_PLAN,
              """+ cdosConfigMap.configuration_map("MTNWEST_MIDWEST_CALI_BENEFIT_PLAN_KEY_ELIGIBILITY_CONDITION")+""" AS BENEFIT_PLAN_KEY,
              e.BENEFIT_PLAN_DESC AS BENEFIT_PLAN_DESC,
              e.CONTRACT_NUMBER AS Contract,
              e.PLAN_BENEFIT_PACKAGE AS PBP,
              upper(BTXWALK.STANDARD_VALUE) AS BUSINESS_TYPE,
              e.LINE_OF_BUSINESS AS BUSINESS_LINE,
              CASE WHEN e.BENEFIT_PLAN_EFFECTIVE_DT < '1900-01-01' THEN NULL
                ELSE e.BENEFIT_PLAN_EFFECTIVE_DT END AS BENEFIT_PLAN_EFF_DATE,
                CASE WHEN e.BENEFIT_PLAN_TERM_DT < '1900-01-01' THEN '1900-01-01T00:00:00.000+00:00'
                ELSE e.BENEFIT_PLAN_TERM_DT END AS BENEFIT_PLAN_TERM_DATE
              ,ROW_NUMBER() OVER (PARTITION BY e.tenant_sk, e.source_patient_id, e.record_start_dt ORDER BY
    case when e.primary_coverage_flg = 'Y' then 1
    else 0 end DESC, e.aud_upd_dt desc, e.eligibility_sk desc) elig_rank
              from """ + cdosBase.sfSchema + """."""+ CdosConstants.cdosEligibilityMaterialized + timeTravel +
        """ e LEFT JOIN """ + cdosBase.sfSchema + """.PROVIDER_GROUP_ID_XWALK """ + timeTravel + """ PGXWALK ON e.PROVIDER_GROUP_SK = PGXWALK.PROVIDER_GROUP_SK
        AND PGXWALK.DEL_IND = 0 AND e.PROVIDER_GROUP_SK NOT IN ('0','-1') """ +
        join_string + cdosBase.sfSchema + pod_xwalk_string +
        cdosConfigMap.configuration_map("MTNWEST_MIDWEST_CALI_JOIN_REVISED_POD_ID_ELIGIBILITY_CONDITION") +
        cdosBase.sfSchema + """.Provider_Group """ + timeTravel + """ PG on e.PROVIDER_GROUP_SK=PG.PROVIDER_GROUP_SK """ +
        cdosConfigMap.configuration_map("CT_ELIGIBILITY_CONDITION") + """ INNER JOIN """ +
        cdosBase.sfSchema + source_system_string + cdosBase.sfReferenceDatabase + reference_xwalk_string +
        cdosBase.sfSchema + tenant_sk_string + all_del_ind_string + param_tenant_sk_string + TENANT_NUMBER +
        cdosConfigMap.configuration_map("IN_ELIGIBILITY_DEL_IND_CONDITION") +
        benefitplan_string + cdosBase.lookbackPeriod +
        record_end_dt_string + cdosBase.lookbackPeriod +
        trunc_lob_string_in + lob_in + trunc_lob_string_ex + lob_ex + """)""" +
        cdosConfigMap.configuration_map("HEALTH_PLAN_ELIGIBILITY_CONDITION") +
        cdosConfigMap.configuration_map("NJ_HEALTHPLAN_UHC_ELIGIBILITY_CONDITION") +
        source +""") where elig_rank = 1""" +
        cdosConfigMap.configuration_map("NJ_HEALTHPLAN_UHC_UNION_ELIGIBILITY_CONDITION")
  }

  cdosBase.query_writer(targetMarket.toLowerCase, domainName,cdosBase.consumer, targetQuery)

  def runNotebook(): String = {
    println("Running CDOS " + domainName.capitalize + " Notebook")
    cdosBase.setLastExtractDT()

    println(s"CdosEligibility 1 : ${java.time.LocalDateTime.now}")

    val currentDT = cdosBase.getExtractDate_inscope(targetMarket, cdosBase.dbName)

    println(s"CdosEligibility 2 : ${java.time.LocalDateTime.now}")

    val currentDate = currentDT.replace("-", "").substring(2, 8)

    val deltaDF = sfCDCM.sql(targetQuery)

    println(s"CdosEligibility 3 : ${java.time.LocalDateTime.now}")

    cdosBase.dropTableifExists(eligibiltySourceTable)
    println("Dropped eligibilitySourceTable:" + eligibiltySourceTable)
    println("cdosBase.dropTableifExists(eligibilitySourceTable) completed")
    println("Starting: cdosBase.writeDataFrameToSnowflake")
    cdosBase.writeDataFrameToSnowflake(deltaDF, eligibiltySourceTable, "overWrite")
    println("Completed: cdosBase.writeDataFrameToSnowflake")

    println(s"CdosEligibility 4 : ${java.time.LocalDateTime.now}")

    val eligibilityDF_history_withoutdefault = sfCDCM.sql(sqlEligibilityTableDF_history_withoutdefault)
    println("eligibilityDF_history_withoutdefault completed")

    println(s"CdosEligibility 5 : ${java.time.LocalDateTime.now}")

    cdosBase.dropTableifExists(eligibiltyTable)
    println("Dropped eligibilityTable:" + eligibiltyTable)
    println("cdosBase.dropTableifExists(eligibilityTable) completed")
    println("Starting: cdosBase.writeDataFrameToSnowflake")
    cdosBase.writeDataFrameToSnowflake(deltaDF, eligibiltyTable, "overWrite")
    println("Completed: cdosBase.writeDataFrameToSnowflake")

    println(s"CdosEligibility 6 : ${java.time.LocalDateTime.now}")

    val eligibilityDF_history = cdosBase.defaultTable_insertion(cdosBase.targetMarket.toUpperCase, domainName, eligibilityDF_history_withoutdefault)
    println("eligibilityDF_history")

    println(s"CdosEligibility 7 : ${java.time.LocalDateTime.now}")

    val tenantList = tenant.split(",").map(_.trim)
    val columnOrder = eligibilityDF_history.schema.fields.map(_.name)

    val eligibilityDF_history_updated = if (tenantList.length > 1) {
      eligibilityDF_history.withColumn("TENANT_SK", lit(0)).withColumn("SOURCE_SYSTEM_SK", lit(0)).dropDuplicates()
    } else {
      eligibilityDF_history
    }

    val reorderedHistDf = eligibilityDF_history_updated.select(columnOrder.map(col))
    val eligibilityDF_Delta = reorderedHistDf.drop("SOURCE_SYSTEM_SK").drop("TENANT_SK")
    var lastExtractDate = ""
    try {
      lastExtractDate = cdosBase.getLastExtractDate();
    } catch {
      // Value gets from setLastExtractDT
      case e: StringIndexOutOfBoundsException => lastExtractDate = cdosBase.lastExtractDT.toString.replace("-", "").substring(2, 8)
    }

    // FileName Extraction from Txt file
    val tmForFileName = cdosBase.filename_extraction(tenant, targetMarket.toUpperCase())
    val reportNamePrefix = tmForFileName + "_" + RunType + "_" + domainName.capitalize + "_" + lastExtractDate + "_" + currentDate
    // From Landing to Archive
    cdosBase.archiveExtracts(tmForFileName + "_Hist_" + domainName.capitalize + "_")
    cdosBase.archiveExtracts(tmForFileName + "_" + RunType + "_" + domainName.capitalize + "_")

    val status = cdosBase.generateAndLoadExtractUploadHistory_Delta(reorderedHistDf, eligibilityDF_Delta: SpDataFrame, reportNamePrefix)

    return domainName + status
  }

}