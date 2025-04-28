// Databricks notebook source
// MAGIC %run ./CdosBase

// COMMAND ----------

// MAGIC %run ./CdosConstants

// COMMAND ----------

// MAGIC %run ./CdosBaseMedclaims

// COMMAND ----------

// MAGIC %run ./CdosConfigMapMedclaims

// COMMAND ----------

import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.snowflake.snowpark.Session
import java.time.{ZoneOffset, ZonedDateTime}
import scala.language.{postfixOps, reflectiveCalls}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

class CdosMedclaimHeader(var notebookParams: scala.collection.mutable.Map[String, String], sfCDCM:Session) extends SparkSessionWrapper {

  val domainName = CdosConstants.cdosMedclaimHeaderDomainName
  val cdosBase = new CdosBase(notebookParams, sfCDCM)
  val cdosBaseMedclaims = new CdosBaseMedclaims(notebookParams)
  notebookParams += ("domainName" -> "MEDCLAIM_HEADER")
  notebookParams += ("consumer" -> cdosBaseMedclaims.consumer)
  cdosBaseMedclaims.domainName = domainName
  cdosBase.domainName = domainName
  val CdosConfigMapMedclaims = new CdosConfigMapMedclaims(notebookParams, sfCDCM)
  var targetMarket = notebookParams("targetMarket").toLowerCase()
  var sfWarehouse = notebookParams("sfWarehouse")
  var databricksEnv = notebookParams("databricksEnv")
  var sfEnv = notebookParams("snowflakeEnv")
  var sourceSystem = notebookParams("sourceSystem")
  var RunType = notebookParams("RunType")
  val consumer = notebookParams("consumer")
  var lookbackPeriod = notebookParams("lookbackPeriod")
  val dbName = notebookParams("dbName")
  var tenant = notebookParams("tenant")
  var featureName = dbName.replaceAll("(?i)CAREDATA_EXTRACT_ELT.cdos", "").toLowerCase()
  var medDbName = "cdos" + featureName + "_snowpark"
  val temp_inscope_patient_table = medDbName + "." + "temp_inscope_patient_sp_" + targetMarket
  var temp_details_history_table = medDbName + "." + "details_history_table_temp_" + targetMarket
  var timeTravel = ""
  var timeTravelDate = ""

  println("medDbName: "+ medDbName)
  println("sfWarehouse : "+ sfWarehouse)
  println("TIME TRAVEL STARTING")

  val useSparkLastExtractForTimeTravel = notebookParams("useSparkLastExtractForTimeTravel")
  if (useSparkLastExtractForTimeTravel == "true")
    {timeTravel = cdosBase.getTimeTravel(domainName, targetMarket)}
   println("timeTravel: "+ timeTravel)

  timeTravelDate = cdosBase.getDateForLookbackPeriod(timeTravel)
  println("timeTravelDate: " + timeTravelDate)
  
  val snowflakeParams = cdosBase.setSnowflakeparameters().asInstanceOf[scala.collection.mutable.Map[String, String]]
  val sfOptions = Map(
    "sfUrl" ->  snowflakeParams("sfUrl"),
    "sfUser" -> snowflakeParams("sfUser"),
    "pem_private_key" -> snowflakeParams("pem_private_key"),
    "sfWarehouse" -> snowflakeParams("sfWarehouse"),
    "sfDatabase" ->snowflakeParams("sfDatabase"),
    "SCHEMA" -> "CDCM_REFINED"
   )
   if (notebookParams("targetMarket") != null)
      targetMarket = notebookParams("targetMarket").toLowerCase()

   if (notebookParams("lookbackPeriod") != null)
      lookbackPeriod = notebookParams("lookbackPeriod")

   val medclaimHeaderSourceTable: String = medDbName.concat(".").concat(cdosBaseMedclaims.targetMarket).concat("_").concat(domainName).concat("_source")
   println("medclaimHeaderSourceTable "+medclaimHeaderSourceTable)

   var now = java.time.ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).toString()

   val histdbNameSp = dbName.concat("_HISTORY_VALIDATION")
   val detailshistoryTable = histdbNameSp.toLowerCase.concat("_").concat(targetMarket).concat("_medclaim_details_hist") //  =< 1 year

   var source = if (sourceSystem != "All") "AND clh.SOURCE_SYSTEM_SK in (" + sourceSystem + ")" else ""

   val TENANT_NUMBER = "(" + tenant + ")"

   var targetQuery: String = null

   println(cdosBaseMedclaims.targetMarket)
   println("")
   println(CdosConfigMapMedclaims.sdm_type)
   println("")
   println(CdosConfigMapMedclaims.eid)
   println("")
   if ("SDM2".equals(CdosConfigMapMedclaims.sdm_type)) {
      targetQuery =
      """
           SELECT
                   clh.TENANT_SK,
                   clh.SOURCE_SYSTEM_SK,
                   TRIM(SR.SOURCE_SYSTEM_ABBR) as SOURCE_SYSTEM,
                   TRIM(clh.SOURCE_PATIENT_ID) as ENTITY_MEMBER_ID,
                   clh.CONTRACT_NUMBER AS Contract,
                   clh.PBP AS PBP,
                   TRIM(clh.HEALTH_PLAN_STANDARD_VALUE) as HEALTH_PLAN,
                   TRIM(clh.HEALTH_PLAN_CARD_ID) as HP_MEMBER_ID,
                   CASE WHEN clh.SOURCE_SYSTEM_SK = '610' THEN clh.claim_number
                   ELSE TRIM(concat(clh.source_system_sk,'-',clh.claim_number)) END AS CLAIM_NUMBER,

                   CASE
                     WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'PAID' THEN 'P'
                     WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'DENIED' THEN 'D'
                     WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'OTHER' THEN 'O'
                     WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'PENDED' THEN ''
                   END as CLAIM_STATUS,

                   TRIM(clh.CLAIM_TYPE_CD) as CLAIM_TYPE,
                   CASE
                   WHEN clh.DEL_IND = 1 THEN 'DELETE'
                   ELSE clh.CLAIM_ADJUST_TYPE END as CLAIM_ADJUST_TYPE,

                   CASE WHEN clh.SOURCE_SYSTEM_SK = '610' AND CLAIM_ADJUST_TYPE NOT IN('ORIGINAL', 'ADJUSTMENT') THEN TRIM(clh.ORIG_CLAIM_NBR)
                        WHEN clh.SOURCE_SYSTEM_SK <> '610' AND CLAIM_ADJUST_TYPE NOT IN('ORIGINAL', 'ADJUSTMENT') THEN TRIM(concat(clh.SOURCE_SYSTEM_SK,'-',clh.ORIG_CLAIM_NBR))
                        ELSE '' END AS ORIG_CLAIM_NUMBER,
                   CASE WHEN clh.ADM_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                      ELSE clh.ADM_DT END AS ADMIT_DATE,
                   CASE WHEN clh.DISCH_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                      ELSE clh.DISCH_DT END AS DISCHARGE_DATE,

                   CASE
                     WHEN clh.SERV_FROM_DT is null THEN clh.SERVICE_DT
                     WHEN clh.SERV_FROM_DT is not null THEN clh.SERV_FROM_DT
                     WHEN clh.SERV_FROM_DT is null and clh.SERVICE_DT < '1900-01-01T00:00:00.000+00:00' THEN '1900-01-01T00:00:00.000+00:00'
                     WHEN clh.SERV_FROM_DT is not null and clh.SERV_FROM_DT < '1900-01-01T00:00:00.000+00:00' THEN '1900-01-01T00:00:00.000+00:00'
                   END as STATEMENT_FROM_DATE,

                   CASE
                     WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is not null THEN clh.SERV_FROM_DT
                     WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is null THEN clh.SERVICE_DT
                     WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is not null AND clh.SERV_FROM_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                     WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is null AND clh.SERVICE_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                     WHEN clh.SERV_THRU_DT is not null AND clh.SERV_THRU_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                     ELSE clh.SERV_THRU_DT
                   END as STATEMENT_TO_DATE,

                   TRIM(clh.BILL_TYPE) as BILL_TYPE,
                   TRIM(clh.INPATIENT_FLG) as INPATIENT_FLAG,
                   TRIM(clh.DRG_CODE) as DRG_CODE,
                   CASE
                      WHEN  RENPG.PROVIDER_GROUP_ID = '-1' THEN '0'
                      WHEN  RENPG.PROVIDER_GROUP_ID = 'NS' THEN '0'
                      WHEN  RENPG.PROVIDER_GROUP_ID = 'NF' THEN '0'
                      WHEN  RENPG.PROVIDER_GROUP_ID is null THEN '0'
                      WHEN TRUE THEN NVL(PGXWALK.REVISED_PROVIDER_GROUP_ID,'0') END AS RENDERING_PROVIDER_GROUP_ID,

                   TRIM(cldp.ADMIT_DIAG_CD) as ICD_DIAG_ADMIT,

                   TRIM(cldp.ICD_DIAG_CD_1) as ICD_DIAG_1,
                   CASE
                    WHEN ICD_DIAG_CD_1 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_1_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                   END as DIAG_CODE_1_POA,

                   TRIM(cldp.ICD_DIAG_CD_2) as ICD_DIAG_2,
                   CASE
                    WHEN ICD_DIAG_CD_2 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_2_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_2_POA,

                   TRIM(cldp.ICD_DIAG_CD_3) as ICD_DIAG_3,
                   CASE
                    WHEN ICD_DIAG_CD_3 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_3_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_3_POA,

                   TRIM(cldp.ICD_DIAG_CD_4) as ICD_DIAG_4,
                   CASE
                    WHEN ICD_DIAG_CD_4 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_4_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_4_POA,

                   TRIM(cldp.ICD_DIAG_CD_5) as ICD_DIAG_5,
                   CASE
                    WHEN ICD_DIAG_CD_5 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_5_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_5_POA,

                   TRIM(cldp.ICD_DIAG_CD_6) as ICD_DIAG_6,
                   CASE
                    WHEN ICD_DIAG_CD_6 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_6_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_6_POA,

                   TRIM(cldp.ICD_DIAG_CD_7) as ICD_DIAG_7,
                   CASE
                    WHEN ICD_DIAG_CD_7 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_7_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_7_POA,

                   TRIM(cldp.ICD_DIAG_CD_8) as ICD_DIAG_8,
                   CASE
                    WHEN ICD_DIAG_CD_8 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_8_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_8_POA,

                   TRIM(cldp.ICD_DIAG_CD_9) as ICD_DIAG_9,
                   CASE
                    WHEN ICD_DIAG_CD_9 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_9_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_9_POA,

                   TRIM(cldp.ICD_DIAG_CD_10) as ICD_DIAG_10,
                   CASE
                    WHEN ICD_DIAG_CD_10 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_10_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_10_POA,

                   TRIM(cldp.ICD_DIAG_CD_11) as ICD_DIAG_11,
                   CASE
                    WHEN ICD_DIAG_CD_11 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_11_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_11_POA,

                   TRIM(cldp.ICD_DIAG_CD_12) as ICD_DIAG_12,
                   CASE
                    WHEN ICD_DIAG_CD_12 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_12_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_12_POA,

                   TRIM(cldp.ICD_DIAG_CD_13) as ICD_DIAG_13,
                   CASE
                    WHEN ICD_DIAG_CD_13 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_13_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_13_POA,

                   TRIM(cldp.ICD_DIAG_CD_14) as ICD_DIAG_14,
                   CASE
                    WHEN ICD_DIAG_CD_14 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_14_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_14_POA,

                   TRIM(cldp.ICD_DIAG_CD_15) as ICD_DIAG_15,
                   CASE
                    WHEN ICD_DIAG_CD_15 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_15_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_15_POA,

                   TRIM(cldp.ICD_DIAG_CD_16) as ICD_DIAG_16,
                   CASE
                    WHEN ICD_DIAG_CD_16 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_16_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_16_POA,

                   TRIM(cldp.ICD_DIAG_CD_17) as ICD_DIAG_17,
                   CASE
                    WHEN ICD_DIAG_CD_17 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_17_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_17_POA,

                   TRIM(cldp.ICD_DIAG_CD_18) as ICD_DIAG_18,
                   CASE
                    WHEN ICD_DIAG_CD_18 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_18_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_18_POA,

                   TRIM(cldp.ICD_DIAG_CD_19) as ICD_DIAG_19,
                   CASE
                    WHEN ICD_DIAG_CD_19 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_19_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_19_POA,

                   TRIM(cldp.ICD_DIAG_CD_20) as ICD_DIAG_20,
                   CASE
                    WHEN ICD_DIAG_CD_20 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_20_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_20_POA,

                   TRIM(cldp.ICD_DIAG_CD_21) as ICD_DIAG_21,
                   CASE
                    WHEN ICD_DIAG_CD_21 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_21_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_21_POA,

                   TRIM(cldp.ICD_DIAG_CD_22) as ICD_DIAG_22,
                   CASE
                    WHEN ICD_DIAG_CD_22 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_22_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_22_POA,

                   TRIM(cldp.ICD_DIAG_CD_23) as ICD_DIAG_23,
                   CASE
                    WHEN ICD_DIAG_CD_23 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_23_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_23_POA,

                   TRIM(cldp.ICD_DIAG_CD_24) as ICD_DIAG_24,
                   CASE
                    WHEN ICD_DIAG_CD_24 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_24_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_24_POA,

                   TRIM(cldp.ICD_DIAG_CD_25) as ICD_DIAG_25,
                   CASE
                    WHEN ICD_DIAG_CD_25 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_25_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_25_POA,

                   TRIM(cldp.ICD_DIAG_CD_26) as ICD_DIAG_26,
                   CASE
                    WHEN ICD_DIAG_CD_26 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_26_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_26_POA,

                   TRIM(cldp.ICD_DIAG_CD_27) as ICD_DIAG_27,
                   CASE
                    WHEN ICD_DIAG_CD_27 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_27_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_27_POA,

                   TRIM(cldp.ICD_DIAG_CD_28) as ICD_DIAG_28,
                   CASE
                    WHEN ICD_DIAG_CD_28 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_28_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_28_POA,

                   TRIM(cldp.ICD_DIAG_CD_29) as ICD_DIAG_29,
                   CASE
                    WHEN ICD_DIAG_CD_29 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_29_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_29_POA,

                   TRIM(cldp.ICD_DIAG_CD_30) as ICD_DIAG_30,
                   CASE
                    WHEN ICD_DIAG_CD_30 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_30_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_30_POA,

                   TRIM(cldp.ICD_DIAG_CD_31) as ICD_DIAG_31,
                   CASE
                    WHEN ICD_DIAG_CD_31 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_31_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_31_POA,

                   TRIM(cldp.ICD_DIAG_CD_32) as ICD_DIAG_32,
                   CASE
                    WHEN ICD_DIAG_CD_32 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_32_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_32_POA,

                   TRIM(cldp.ICD_DIAG_CD_33) as ICD_DIAG_33,
                   CASE
                    WHEN ICD_DIAG_CD_33 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_33_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_33_POA,

                   TRIM(cldp.ICD_DIAG_CD_34) as ICD_DIAG_34,
                   CASE
                    WHEN ICD_DIAG_CD_34 is not null THEN
                       CASE
                           WHEN cldp.DIAG_CODE_34_POA = 'Y' THEN 'Y'
                           ELSE 'N'
                       END
                    ELSE null
                    END as DIAG_CODE_34_POA,

                   TRIM(clh.CMS_POS) as FACILITY_TYPE,
                   NVL(cld.CHG_AMT,'0.00') AS CHARGE_AMOUNT,

                   TRIM(clh.SOURCE_REND_PROV_NPI) as RENDERING_PROVIDER_NPI,
                   TRIM(clh.SOURCE_REFR_PROV_NPI) as REFERRING_PROVIDER_NPI,

                   TRIM(clh.REFERRAL_NUM) as REFERRAL_NUMBER,

                   CASE
                      WHEN REN.PROVIDER_ID = '-1' THEN '0'
                      WHEN REN.PROVIDER_ID = 'NS' THEN '0'
                      WHEN REN.PROVIDER_ID = 'NF' THEN '0'
                      WHEN REN.PROVIDER_ID is null THEN '0'
                      ELSE SUBSTRING(TRIM(REN.PROVIDER_ID),1,24)
                   END as RENDERING_PROVIDER_ID,

                   CASE
                      WHEN REF.PROVIDER_ID = '-1' THEN '0'
                      WHEN REF.PROVIDER_ID = 'NS' THEN '0'
                      WHEN REF.PROVIDER_ID = 'NF' THEN '0'
                      WHEN REF.PROVIDER_ID is null THEN '0'
                      ELSE SUBSTRING(TRIM(REF.PROVIDER_ID),1,24)
                   END as REFERRING_PROVIDER_ID,

                   TRIM(clh.PAY_TO_PROV_TIN) as PAYTO_PROVIDER_TIN,
                   TRIM(clh.REND_PROV_CMS_SPEC_CD) as SPECIALTY_CODE,
                   TRIM(clh.REND_PROV_CMS_SPEC_DESC) as SPECIALTY_NAME,

                   TRIM(clh.REFERRAL_NUM) as AUTHORIZATION_NUMBER,

                   TRIM(clh.ADMIT_TYPE) as TYPE_OF_ADMIT,
                   TRIM(clh.ADMIT_SRC) as ADMIT_SOURCE,
                   TRIM(clh.DISCH_STATUS) as DISCHARGE_STATUS,
                   cast((CASE WHEN clh.STATISTICAL_IND=1 THEN 'Y' ELSE 'N' END) as varchar(1)) as Capped_Indicator,
                   TRIM(clpp.ICD_PROC_1) as ICD_PROC_1,
                   TRIM(clpp.ICD_PROC_2) as ICD_PROC_2,
                   TRIM(clpp.ICD_PROC_3) as ICD_PROC_3,
                   TRIM(clpp.ICD_PROC_4) as ICD_PROC_4,
                   TRIM(clpp.ICD_PROC_5) as ICD_PROC_5,
                   TRIM(clpp.ICD_PROC_6) as ICD_PROC_6,
                   TRIM(clh.E_CODE_1) as E_CODE_1,
                   TRIM(clh.E_CODE_2) as E_CODE_2,
                   TRIM(clh.E_CODE_3) as E_CODE_3,

                   IFF(clh.SERV_FROM_DT < '2015-10-01', 9, 0) as ICD_CODE_TYPE,

                   clh.SOURCE_PATIENT_ID,
                   '' as UPDATE_TYPE,
                   clh.SERV_FROM_DT as SERV_FROM_DT,
                   clh.DEL_IND,

                   -- Create a hash based on the columns that will be used in the report.
                   hash(SOURCE_SYSTEM, ENTITY_MEMBER_ID, CONTRACT,
                   PBP, HEALTH_PLAN, HP_MEMBER_ID,
                   clh.CLAIM_NUMBER, CLAIM_STATUS, CLAIM_TYPE,
                   CLAIM_ADJUST_TYPE,
                   ORIG_CLAIM_NUMBER, ADMIT_DATE, DISCHARGE_DATE, STATEMENT_FROM_DATE, STATEMENT_TO_DATE, BILL_TYPE,
                   INPATIENT_FLAG, DRG_CODE, RENPG.PROVIDER_GROUP_ID, ICD_DIAG_ADMIT,
                   ICD_DIAG_1, DIAG_CODE_1_POA, ICD_DIAG_2, DIAG_CODE_2_POA,
                   ICD_DIAG_3, DIAG_CODE_3_POA, ICD_DIAG_4, DIAG_CODE_4_POA,
                   ICD_DIAG_5, DIAG_CODE_5_POA, ICD_DIAG_6, DIAG_CODE_6_POA,
                   ICD_DIAG_7, DIAG_CODE_7_POA, ICD_DIAG_8, DIAG_CODE_8_POA,
                   ICD_DIAG_9, DIAG_CODE_9_POA, ICD_DIAG_10, DIAG_CODE_10_POA,
                   ICD_DIAG_11, DIAG_CODE_11_POA, ICD_DIAG_12, DIAG_CODE_12_POA,
                   ICD_DIAG_13, DIAG_CODE_13_POA, ICD_DIAG_14, DIAG_CODE_14_POA,
                   ICD_DIAG_15, DIAG_CODE_15_POA, ICD_DIAG_16, DIAG_CODE_16_POA,
                   ICD_DIAG_17, DIAG_CODE_17_POA, ICD_DIAG_18, DIAG_CODE_18_POA,
                   ICD_DIAG_19, DIAG_CODE_19_POA, ICD_DIAG_20, DIAG_CODE_20_POA,
                   ICD_DIAG_21, DIAG_CODE_21_POA, ICD_DIAG_22, DIAG_CODE_22_POA,
                   ICD_DIAG_23, DIAG_CODE_23_POA, ICD_DIAG_24, DIAG_CODE_24_POA,
                   ICD_DIAG_25, DIAG_CODE_25_POA, ICD_DIAG_26, DIAG_CODE_26_POA,
                   ICD_DIAG_27, DIAG_CODE_27_POA, ICD_DIAG_28, DIAG_CODE_28_POA,
                   ICD_DIAG_29, DIAG_CODE_29_POA, ICD_DIAG_30, DIAG_CODE_30_POA,
                   ICD_DIAG_31, DIAG_CODE_31_POA, ICD_DIAG_32, DIAG_CODE_32_POA,
                   ICD_DIAG_33, DIAG_CODE_33_POA, ICD_DIAG_34, DIAG_CODE_34_POA,
                   FACILITY_TYPE, CHARGE_AMOUNT, RENDERING_PROVIDER_NPI, REFERRING_PROVIDER_NPI,
                   REFERRAL_NUMBER, REN.PROVIDER_ID, REF.PROVIDER_ID, PAYTO_PROVIDER_TIN,
                   SPECIALTY_CODE, SPECIALTY_NAME, AUTHORIZATION_NUMBER, TYPE_OF_ADMIT,
                   ADMIT_SOURCE, DISCHARGE_STATUS, CAPPED_INDICATOR, ICD_PROC_1,
                   ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, E_CODE_1, E_CODE_2, E_CODE_3,clh.DEL_IND) as HASH
             FROM """ + cdosBaseMedclaims.sfSchema + """.""" + CdosConstants.cdosMedclaimHeaderMaterialized + timeTravel+""" clh
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.Claim_Diagnosis_Pivot """+timeTravel+""" cldp on clh.CLAIM_HEADER_SK=cldp.CLAIM_HEADER_SK and cldp.del_ind = 0
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.Claim_Procedure_Pivot """+timeTravel+""" clpp on clh.CLAIM_HEADER_SK=clpp.CLAIM_HEADER_SK and clpp.del_ind = 0
             LEFT OUTER JOIN (select CLAIM_HEADER_SK,sum(CHG_AMT) as CHG_AMT from """ + cdosBaseMedclaims.sfSchema + """.""" + CdosConstants.cdosMedclaimDetailsMaterialized + timeTravel+""" where DEL_IND=0 group by CLAIM_HEADER_SK) cld on clh.CLAIM_HEADER_SK = cld.CLAIM_HEADER_SK
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.SOURCE_SYSTEM"""+timeTravel+""" SR ON clh.SOURCE_SYSTEM_SK=SR.SOURCE_SYSTEM_SK
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfReferenceDatabase + """.OPTUMCARE_REFERENCE.VW_TGT_MAPPED_CODE_VALUE"""+timeTravel+""" CW_CLMST on clh.CLAIM_STATUS_CD=CW_CLMST.STD_CD_VAL and
             CW_CLMST.SYS_OF_CTRL_NAME = 'CDOS' and CW_CLMST.TARGET_CD_SET_NAME = 'CDOS_CDP_CLAIM_STATUS_CROSSWALK'
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.PROVIDER """+timeTravel+"""REN ON clh.RENDERING_PROV_SK = REN.PROVIDER_SK
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.PROVIDER """+timeTravel+"""REF on clh.REFR_PROV_SK = REF.PROVIDER_SK
             LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.PROVIDER_GROUP """+timeTravel+"""RENPG on clh.PROVIDER_GROUP_SK = RENPG.PROVIDER_GROUP_SK
             LEFT JOIN CDCM_REFINED.PROVIDER_GROUP_ID_XWALK """+timeTravel+ """PGXWALK ON RENPG.PROVIDER_GROUP_SK = PGXWALK.PROVIDER_GROUP_SK AND PGXWALK.DEL_IND = 0 AND RENPG.PROVIDER_GROUP_SK NOT IN ('0','-1')
             WHERE clh.TENANT_SK IN """ + TENANT_NUMBER + """
             AND clh.SERV_FROM_DT >= to_timestamp_ntz(dateadd('month', -""" + lookbackPeriod + """, DATE_TRUNC('MONTH',""" + timeTravelDate + """)))
             AND ifnull(clh.FINAL_STATUS_FLG,'Y')='Y' """ + source

  }
  else if ("SDM3".equals(CdosConfigMapMedclaims.sdm_type)) {
    targetQuery =
      """
           SELECT
                    clh.TENANT_SK,
                    clh.SOURCE_SYSTEM_SK,
                    TRIM(SR.SOURCE_SYSTEM_ABBR) as SOURCE_SYSTEM,
                    TRIM(clh.SOURCE_PATIENT_ID) as ENTITY_MEMBER_ID,
                    CASE
                    WHEN (TRIM(clh.CONTRACT_NUMBER)  =  '-1' OR TRIM(clh.CONTRACT_NUMBER) = 'OCN' OR clh.CONTRACT_NUMBER IS NULL)  THEN
                    '0'
                    ELSE TRIM(clh.CONTRACT_NUMBER)
                    END  as Contract,

                    CASE
                    WHEN (TRIM(clh.PBP)  =  '-1' OR TRIM(clh.PBP) = 'CT' OR clh.PBP IS NULL)  THEN
                    '0'
                    ELSE TRIM(clh.PBP)
                    END  as PBP,
                    substring(TRIM(clh.HEALTH_PLAN_STANDARD_VALUE),1,15)  as HEALTH_PLAN,
                    CASE WHEN LENGTH(TRIM(clh.HEALTH_PLAN_CARD_ID)) > 20 THEN NULL ELSE TRIM(clh.HEALTH_PLAN_CARD_ID) END AS HP_MEMBER_ID,

                    CASE WHEN clh.SOURCE_SYSTEM_SK = '610' THEN clh.claim_number
                    ELSE TRIM(concat(clh.source_system_sk,'-',clh.claim_number)) END AS CLAIM_NUMBER,
                    CASE
                      WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'PAID' THEN 'P'
                      WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'DENIED' THEN 'D'
                      WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'OTHER' THEN 'O'
                      WHEN TRIM(UPPER(CW_CLMST.TARGET_CD_VAL)) = 'PENDED' THEN ''
                      ELSE 'U'
                    END as CLAIM_STATUS,

                    TRIM(clh.CLAIM_TYPE_CD) as CLAIM_TYPE,

                      CASE
                    WHEN clh.DEL_IND = 1 THEN 'DELETE'
                    WHEN clh.CLAIM_ADJUST_TYPE IS NULL THEN 'UNK'
                    ELSE clh.CLAIM_ADJUST_TYPE END as CLAIM_ADJUST_TYPE,

                     CASE WHEN clh.SOURCE_SYSTEM_SK = '610' AND CLAIM_ADJUST_TYPE NOT IN('ORIGINAL', 'ADJUSTMENT') THEN TRIM(clh.ORIG_CLAIM_NBR)
                         WHEN clh.SOURCE_SYSTEM_SK <> '610' AND CLAIM_ADJUST_TYPE NOT IN('ORIGINAL', 'ADJUSTMENT') THEN TRIM(concat(clh.SOURCE_SYSTEM_SK,'-',clh.ORIG_CLAIM_NBR))
                         ELSE '' END AS ORIG_CLAIM_NUMBER,
                    CASE
                    WHEN clh.ADM_DT =  '1753-01-01T00:00:00' THEN NULL
                    WHEN clh.ADM_DT =  '1111-01-08T00:00:00' THEN NULL
                    WHEN clh.ADM_DT = '8888-12-31T00:00:00' THEN NULL
                    WHEN clh.ADM_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                    ELSE clh.ADM_DT
                    END as ADMIT_DATE ,

                    CASE
                    WHEN clh.DISCH_DT =  '1753-01-01T00:00:00' THEN NULL
                    WHEN clh.DISCH_DT =  '1111-01-08T00:00:00' THEN NULL
                    WHEN clh.DISCH_DT = '8888-12-31T00:00:00' THEN NULL
                    WHEN clh.DISCH_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                    ELSE clh.DISCH_DT
                    END as DISCHARGE_DATE ,

                    CASE
                      WHEN clh.SERV_FROM_DT is null THEN clh.SERVICE_DT
                      WHEN clh.SERV_FROM_DT is not null THEN clh.SERV_FROM_DT
                      WHEN clh.SERV_FROM_DT is null and clh.SERVICE_DT < '1900-01-01T00:00:00.000+00:00' THEN '1900-01-01T00:00:00.000+00:00'
                      WHEN clh.SERV_FROM_DT is not null and clh.SERV_FROM_DT < '1900-01-01T00:00:00.000+00:00' THEN '1900-01-01T00:00:00.000+00:00'
                    END as STATEMENT_FROM_DATE,

                    CASE
                      WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is not null THEN clh.SERV_FROM_DT
                      WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is null THEN clh.SERVICE_DT
                      WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is not null AND clh.SERV_FROM_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                      WHEN clh.SERV_THRU_DT is null AND clh.SERV_FROM_DT is null AND clh.SERVICE_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                      WHEN clh.SERV_THRU_DT is not null AND clh.SERV_THRU_DT < '1900-01-01T00:00:00.000+00:00' THEN NULL
                      ELSE clh.SERV_THRU_DT
                    END as STATEMENT_TO_DATE,

                    TRIM(clh.BILL_TYPE) as BILL_TYPE,
                    TRIM(clh.INPATIENT_FLG) as INPATIENT_FLAG,
                    TRIM(clh.DRG_CODE) as DRG_CODE,
                    CASE
                       WHEN  RENPG.PROVIDER_GROUP_ID = '-1' THEN '0'
                       WHEN  RENPG.PROVIDER_GROUP_ID = 'NS' THEN '0'
                       WHEN  RENPG.PROVIDER_GROUP_ID = 'NF' THEN '0'
                       WHEN  RENPG.PROVIDER_GROUP_ID is null THEN '0'
                       WHEN TRUE THEN NVL(PGXWALK.REVISED_PROVIDER_GROUP_ID,'0') END AS RENDERING_PROVIDER_GROUP_ID,

                    TRIM(cldp.ADMIT_DIAG_CD) as ICD_DIAG_ADMIT,

                   CASE
                    WHEN cldp.ICD_DIAG_CD_1 IS NULL
                    THEN 'UNK'
                    ELSE TRIM(cldp.ICD_DIAG_CD_1)
                    END as ICD_DIAG_1,
                    CASE
                     WHEN ICD_DIAG_CD_1 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_1_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                    END as DIAG_CODE_1_POA,

                    TRIM(cldp.ICD_DIAG_CD_2) as ICD_DIAG_2,
                    CASE
                     WHEN ICD_DIAG_CD_2 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_2_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_2_POA,

                    TRIM(cldp.ICD_DIAG_CD_3) as ICD_DIAG_3,
                    CASE
                     WHEN ICD_DIAG_CD_3 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_3_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_3_POA,

                    TRIM(cldp.ICD_DIAG_CD_4) as ICD_DIAG_4,
                    CASE
                     WHEN ICD_DIAG_CD_4 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_4_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_4_POA,

                    TRIM(cldp.ICD_DIAG_CD_5) as ICD_DIAG_5,
                    CASE
                     WHEN ICD_DIAG_CD_5 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_5_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_5_POA,

                    TRIM(cldp.ICD_DIAG_CD_6) as ICD_DIAG_6,
                    CASE
                     WHEN ICD_DIAG_CD_6 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_6_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_6_POA,

                    TRIM(cldp.ICD_DIAG_CD_7) as ICD_DIAG_7,
                    CASE
                     WHEN ICD_DIAG_CD_7 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_7_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_7_POA,

                    TRIM(cldp.ICD_DIAG_CD_8) as ICD_DIAG_8,
                    CASE
                     WHEN ICD_DIAG_CD_8 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_8_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_8_POA,

                    TRIM(cldp.ICD_DIAG_CD_9) as ICD_DIAG_9,
                    CASE
                     WHEN ICD_DIAG_CD_9 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_9_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_9_POA,

                    TRIM(cldp.ICD_DIAG_CD_10) as ICD_DIAG_10,
                    CASE
                     WHEN ICD_DIAG_CD_10 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_10_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_10_POA,

                    TRIM(cldp.ICD_DIAG_CD_11) as ICD_DIAG_11,
                    CASE
                     WHEN ICD_DIAG_CD_11 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_11_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_11_POA,

                    TRIM(cldp.ICD_DIAG_CD_12) as ICD_DIAG_12,
                    CASE
                     WHEN ICD_DIAG_CD_12 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_12_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_12_POA,

                    TRIM(cldp.ICD_DIAG_CD_13) as ICD_DIAG_13,
                    CASE
                     WHEN ICD_DIAG_CD_13 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_13_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_13_POA,

                    TRIM(cldp.ICD_DIAG_CD_14) as ICD_DIAG_14,
                    CASE
                     WHEN ICD_DIAG_CD_14 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_14_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_14_POA,

                    TRIM(cldp.ICD_DIAG_CD_15) as ICD_DIAG_15,
                    CASE
                     WHEN ICD_DIAG_CD_15 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_15_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_15_POA,

                    TRIM(cldp.ICD_DIAG_CD_16) as ICD_DIAG_16,
                    CASE
                     WHEN ICD_DIAG_CD_16 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_16_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_16_POA,

                    TRIM(cldp.ICD_DIAG_CD_17) as ICD_DIAG_17,
                    CASE
                     WHEN ICD_DIAG_CD_17 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_17_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_17_POA,

                    TRIM(cldp.ICD_DIAG_CD_18) as ICD_DIAG_18,
                    CASE
                     WHEN ICD_DIAG_CD_18 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_18_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_18_POA,

                    TRIM(cldp.ICD_DIAG_CD_19) as ICD_DIAG_19,
                    CASE
                     WHEN ICD_DIAG_CD_19 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_19_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_19_POA,

                    TRIM(cldp.ICD_DIAG_CD_20) as ICD_DIAG_20,
                    CASE
                     WHEN ICD_DIAG_CD_20 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_20_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_20_POA,

                    TRIM(cldp.ICD_DIAG_CD_21) as ICD_DIAG_21,
                    CASE
                     WHEN ICD_DIAG_CD_21 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_21_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_21_POA,

                    TRIM(cldp.ICD_DIAG_CD_22) as ICD_DIAG_22,
                    CASE
                     WHEN ICD_DIAG_CD_22 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_22_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_22_POA,

                    TRIM(cldp.ICD_DIAG_CD_23) as ICD_DIAG_23,
                    CASE
                     WHEN ICD_DIAG_CD_23 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_23_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_23_POA,

                    TRIM(cldp.ICD_DIAG_CD_24) as ICD_DIAG_24,
                    CASE
                     WHEN ICD_DIAG_CD_24 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_24_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_24_POA,

                    TRIM(cldp.ICD_DIAG_CD_25) as ICD_DIAG_25,
                    CASE
                     WHEN ICD_DIAG_CD_25 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_25_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_25_POA,

                    TRIM(cldp.ICD_DIAG_CD_26) as ICD_DIAG_26,
                    CASE
                     WHEN ICD_DIAG_CD_26 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_26_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_26_POA,

                    TRIM(cldp.ICD_DIAG_CD_27) as ICD_DIAG_27,
                    CASE
                     WHEN ICD_DIAG_CD_27 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_27_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_27_POA,

                    TRIM(cldp.ICD_DIAG_CD_28) as ICD_DIAG_28,
                    CASE
                     WHEN ICD_DIAG_CD_28 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_28_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_28_POA,

                    TRIM(cldp.ICD_DIAG_CD_29) as ICD_DIAG_29,
                    CASE
                     WHEN ICD_DIAG_CD_29 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_29_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_29_POA,

                    TRIM(cldp.ICD_DIAG_CD_30) as ICD_DIAG_30,
                    CASE
                     WHEN ICD_DIAG_CD_30 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_30_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_30_POA,

                    TRIM(cldp.ICD_DIAG_CD_31) as ICD_DIAG_31,
                    CASE
                     WHEN ICD_DIAG_CD_31 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_31_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_31_POA,

                    TRIM(cldp.ICD_DIAG_CD_32) as ICD_DIAG_32,
                    CASE
                     WHEN ICD_DIAG_CD_32 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_32_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_32_POA,

                    TRIM(cldp.ICD_DIAG_CD_33) as ICD_DIAG_33,
                    CASE
                     WHEN ICD_DIAG_CD_33 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_33_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_33_POA,

                    TRIM(cldp.ICD_DIAG_CD_34) as ICD_DIAG_34,
                    CASE
                     WHEN ICD_DIAG_CD_34 is not null THEN
                        CASE
                            WHEN cldp.DIAG_CODE_34_POA = 'Y' THEN 'Y'
                            ELSE 'N'
                        END
                     ELSE null
                     END as DIAG_CODE_34_POA,

                    TRIM(clh.CMS_POS) as FACILITY_TYPE,
                    CASE WHEN LENGTH(TRIM(cld.CHG_AMT)) > 13 THEN NULL ELSE TRIM(cld.CHG_AMT) END AS CHARGE_AMOUNT,

                    TRIM(clh.SOURCE_REND_PROV_NPI) as RENDERING_PROVIDER_NPI,
                    TRIM(clh.SOURCE_REFR_PROV_NPI) as REFERRING_PROVIDER_NPI,

                    TRIM(clh.REFERRAL_NUM) as REFERRAL_NUMBER,

                    CASE
                       WHEN REN.PROVIDER_ID = '-1' THEN '0'
                       WHEN REN.PROVIDER_ID = 'NS' THEN '0'
                       WHEN REN.PROVIDER_ID = 'NF' THEN '0'
                       WHEN REN.PROVIDER_ID is null THEN '0'
                       ELSE SUBSTRING(TRIM(REN.PROVIDER_ID),1,24)
                    END as RENDERING_PROVIDER_ID,

                    CASE
                       WHEN REF.PROVIDER_ID = '-1' THEN '0'
                       WHEN REF.PROVIDER_ID = 'NS' THEN '0'
                       WHEN REF.PROVIDER_ID = 'NF' THEN '0'
                       WHEN REF.PROVIDER_ID is null THEN '0'
                       ELSE SUBSTRING(TRIM(REF.PROVIDER_ID),1,24)
                    END as REFERRING_PROVIDER_ID,
                    TRIM(clh.PAY_TO_PROV_TIN) as PAYTO_PROVIDER_TIN,
                    TRIM(clh.REND_PROV_CMS_SPEC_CD) as SPECIALTY_CODE,
                    TRIM(clh.REND_PROV_CMS_SPEC_DESC) as SPECIALTY_NAME,
                    TRIM(clh.REFERRAL_NUM) as AUTHORIZATION_NUMBER,
                    TRIM(clh.ADMIT_TYPE) as TYPE_OF_ADMIT,
                    TRIM(clh.ADMIT_SRC) as ADMIT_SOURCE,
                    TRIM(clh.DISCH_STATUS) as DISCHARGE_STATUS,
                    cast((CASE WHEN clh.STATISTICAL_IND=1 THEN 'Y' ELSE 'N' END) as varchar(1)) as Capped_Indicator,
                    TRIM(clpp.ICD_PROC_1) as ICD_PROC_1,
                    TRIM(clpp.ICD_PROC_2) as ICD_PROC_2,
                    TRIM(clpp.ICD_PROC_3) as ICD_PROC_3,
                    TRIM(clpp.ICD_PROC_4) as ICD_PROC_4,
                    TRIM(clpp.ICD_PROC_5) as ICD_PROC_5,
                    TRIM(clpp.ICD_PROC_6) as ICD_PROC_6,
                    TRIM(clh.E_CODE_1) as E_CODE_1,
                    TRIM(clh.E_CODE_2) as E_CODE_2,
                    TRIM(clh.E_CODE_3) as E_CODE_3,
                    IFF(clh.SERV_FROM_DT < '2015-10-01', 9, 0) as ICD_CODE_TYPE,
                    clh.SOURCE_PATIENT_ID,
                    '' as UPDATE_TYPE,
                    clh.SERV_FROM_DT as SERV_FROM_DT,
                    clh.DEL_IND,
                    -- Create a hash based on the columns that will be used in the report.
                    hash(SOURCE_SYSTEM, ENTITY_MEMBER_ID, CONTRACT,
                    PBP, HEALTH_PLAN, HP_MEMBER_ID,
                    clh.CLAIM_NUMBER, CLAIM_STATUS, CLAIM_TYPE,
                    CLAIM_ADJUST_TYPE,
                    ORIG_CLAIM_NUMBER, ADMIT_DATE, DISCHARGE_DATE, STATEMENT_FROM_DATE, STATEMENT_TO_DATE, BILL_TYPE,
                    INPATIENT_FLAG, DRG_CODE, RENPG.PROVIDER_GROUP_ID, ICD_DIAG_ADMIT,
                    ICD_DIAG_1, DIAG_CODE_1_POA, ICD_DIAG_2, DIAG_CODE_2_POA,
                    ICD_DIAG_3, DIAG_CODE_3_POA, ICD_DIAG_4, DIAG_CODE_4_POA,
                    ICD_DIAG_5, DIAG_CODE_5_POA, ICD_DIAG_6, DIAG_CODE_6_POA,
                    ICD_DIAG_7, DIAG_CODE_7_POA, ICD_DIAG_8, DIAG_CODE_8_POA,
                    ICD_DIAG_9, DIAG_CODE_9_POA, ICD_DIAG_10, DIAG_CODE_10_POA,
                    ICD_DIAG_11, DIAG_CODE_11_POA, ICD_DIAG_12, DIAG_CODE_12_POA,
                    ICD_DIAG_13, DIAG_CODE_13_POA, ICD_DIAG_14, DIAG_CODE_14_POA,
                    ICD_DIAG_15, DIAG_CODE_15_POA, ICD_DIAG_16, DIAG_CODE_16_POA,
                    ICD_DIAG_17, DIAG_CODE_17_POA, ICD_DIAG_18, DIAG_CODE_18_POA,
                    ICD_DIAG_19, DIAG_CODE_19_POA, ICD_DIAG_20, DIAG_CODE_20_POA,
                    ICD_DIAG_21, DIAG_CODE_21_POA, ICD_DIAG_22, DIAG_CODE_22_POA,
                    ICD_DIAG_23, DIAG_CODE_23_POA, ICD_DIAG_24, DIAG_CODE_24_POA,
                    ICD_DIAG_25, DIAG_CODE_25_POA, ICD_DIAG_26, DIAG_CODE_26_POA,
                    ICD_DIAG_27, DIAG_CODE_27_POA, ICD_DIAG_28, DIAG_CODE_28_POA,
                    ICD_DIAG_29, DIAG_CODE_29_POA, ICD_DIAG_30, DIAG_CODE_30_POA,
                    ICD_DIAG_31, DIAG_CODE_31_POA, ICD_DIAG_32, DIAG_CODE_32_POA,
                    ICD_DIAG_33, DIAG_CODE_33_POA, ICD_DIAG_34, DIAG_CODE_34_POA,
                    FACILITY_TYPE, CHARGE_AMOUNT, RENDERING_PROVIDER_NPI, REFERRING_PROVIDER_NPI,
                    REFERRAL_NUMBER, REN.PROVIDER_ID, REF.PROVIDER_ID, PAYTO_PROVIDER_TIN,
                    SPECIALTY_CODE, SPECIALTY_NAME, AUTHORIZATION_NUMBER, TYPE_OF_ADMIT,
                    ADMIT_SOURCE, DISCHARGE_STATUS, CAPPED_INDICATOR, ICD_PROC_1,
                    ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, E_CODE_1, E_CODE_2, E_CODE_3,clh.DEL_IND) as HASH
              FROM """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosMedclaimHeaderMaterialized +timeTravel+""" clh
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.Claim_Diagnosis_Pivot """+timeTravel+""" cldp on clh.CLAIM_HEADER_SK=cldp.CLAIM_HEADER_SK and cldp.del_ind = 0
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.Claim_Procedure_Pivot """+timeTravel+"""clpp on clh.CLAIM_HEADER_SK=clpp.CLAIM_HEADER_SK and clpp.del_ind = 0
              LEFT OUTER JOIN (select CLAIM_HEADER_SK,sum(CHG_AMT) as CHG_AMT from """ + cdosBaseMedclaims.sfSchema + """."""+ CdosConstants.cdosMedclaimDetailsMaterialized +timeTravel+""" where DEL_IND=0
              group by CLAIM_HEADER_SK) cld on clh.CLAIM_HEADER_SK = cld.CLAIM_HEADER_SK
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.SOURCE_SYSTEM """+timeTravel+"""SR ON clh.SOURCE_SYSTEM_SK=SR.SOURCE_SYSTEM_SK
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfReferenceDatabase + """.OPTUMCARE_REFERENCE.VW_TGT_MAPPED_CODE_VALUE """+timeTravel+"""CW_CLMST on clh.CLAIM_STATUS_CD=CW_CLMST.STD_CD_VAL and
              CW_CLMST.SYS_OF_CTRL_NAME = 'CDOS' and CW_CLMST.TARGET_CD_SET_NAME = 'CDOS_CDP_CLAIM_STATUS_CROSSWALK'
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.PROVIDER """+timeTravel+"""REN ON clh.RENDERING_PROV_SK = REN.PROVIDER_SK
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.PROVIDER """+timeTravel+"""REF on clh.REFR_PROV_SK = REF.PROVIDER_SK
              LEFT OUTER JOIN """ + cdosBaseMedclaims.sfSchema + """.PROVIDER_GROUP """+timeTravel+"""RENPG on clh.PROVIDER_GROUP_SK = RENPG.PROVIDER_GROUP_SK
              LEFT JOIN CDCM_REFINED.PROVIDER_GROUP_ID_XWALK """+timeTravel+"""PGXWALK ON RENPG.PROVIDER_GROUP_SK = PGXWALK.PROVIDER_GROUP_SK  AND PGXWALK.DEL_IND = 0 AND RENPG.PROVIDER_GROUP_SK NOT IN ('0','-1')
              WHERE clh.TENANT_SK IN """ + TENANT_NUMBER + """
              AND clh.SERV_FROM_DT >= to_timestamp_ntz(dateadd('month', -""" + lookbackPeriod + """, DATE_TRUNC('MONTH',""" + timeTravelDate + """)))
              AND ifnull(clh.FINAL_STATUS_FLG,'Y')='Y' """ + source
  } else ""

   cdosBaseMedclaims.query_writer(targetMarket.toLowerCase, domainName,cdosBaseMedclaims.consumer, targetQuery)
   
   val clmDtlCk =

         """
         select distinct CLAIM_NUMBER FROM medclaim_details_historyDF_withoutdefault_Del_Ind0
         where CLAIM_NUMBER in (select dtl.CLAIM_NUMBER from """ + temp_details_history_table + """ dtl WHERE dtl.loadtime = (select max(dt.loadtime) FROM """ + temp_details_history_table + """ dt))

         UNION ALL

         select distinct CLAIM_NUMBER FROM medclaim_details_historyDF_withoutdefault_Del_Ind1 
         where CLAIM_NUMBER in (select dtl.CLAIM_NUMBER from """ + temp_details_history_table + """ dtl)
         """

  val medclaim_header_historyDF_withoutdefault_both_del_ind =
    """
    SELECT distinct
      TENANT_SK as Tenant_sk,
      SOURCE_SYSTEM_SK as Source_System_sk,
      SOURCE_SYSTEM as Source_System,
      CONTRACT as Contract,
      PBP as Pbp,
      ENTITY_MEMBER_ID as Entity_Member_Id,
      HEALTH_PLAN as Health_Plan,
      HP_MEMBER_ID as Hp_Member_Id,
      CLAIM_NUMBER as Claim_Number,
      CLAIM_STATUS as Claim_Status,
      CLAIM_TYPE as Claim_Type,

      CASE
      WHEN CLAIM_ADJUST_TYPE = 'DELETE' THEN CLAIM_ADJUST_TYPE
      WHEN UPDATE_TYPE = 'ADJUSTMENT' AND (CLAIM_ADJUST_TYPE <> 'BACKOUT' OR UPDATE_TYPE <> 'BACKOUT') THEN 'ADJUSTMENT'
        ELSE CLAIM_ADJUST_TYPE
      END as Claim_Adjust_Type,

      ORIG_CLAIM_NUMBER as Orig_Claim_Number,
      ADMIT_DATE as Admit_Date,
      DISCHARGE_DATE as Discharge_Date,
      STATEMENT_FROM_DATE as Statement_From_Date,
      STATEMENT_TO_DATE as Statement_To_Date,
      BILL_TYPE as Bill_Type,
      INPATIENT_FLAG as Inpatient_Flag,
      DRG_CODE as Drg_Code,
      ICD_DIAG_ADMIT as Icd_Diag_Admit,
      ICD_DIAG_1 as Icd_Diag_1,
      DIAG_CODE_1_POA as Diag_Code_1_Poa,
      ICD_DIAG_2 as Icd_Diag_2,
      DIAG_CODE_2_POA as Diag_Code_2_Poa,
      ICD_DIAG_3 as Icd_Diag_3,
      DIAG_CODE_3_POA as Diag_Code_3_Poa,
      ICD_DIAG_4 as Icd_Diag_4,
      DIAG_CODE_4_POA as Diag_Code_4_Poa,
      ICD_DIAG_5 as Icd_Diag_5,
      DIAG_CODE_5_POA as Diag_Code_5_Poa,
      ICD_DIAG_6 as Icd_Diag_6,
      DIAG_CODE_6_POA as Diag_Code_6_Poa,
      ICD_DIAG_7 as Icd_Diag_7,
      DIAG_CODE_7_POA as Diag_Code_7_Poa,
      ICD_DIAG_8 as Icd_Diag_8,
      DIAG_CODE_8_POA as Diag_Code_8_Poa,
      ICD_DIAG_9 as Icd_Diag_9,
      DIAG_CODE_9_POA as Diag_Code_9_Poa,
      ICD_DIAG_10 as Icd_Diag_10,
      DIAG_CODE_10_POA as Diag_Code_10_Poa,
      ICD_DIAG_11 as Icd_Diag_11,
      DIAG_CODE_11_POA as Diag_Code_11_Poa,
      ICD_DIAG_12 as Icd_Diag_12,
      DIAG_CODE_12_POA as Diag_Code_12_Poa,
      ICD_DIAG_13 as Icd_Diag_13,
      DIAG_CODE_13_POA as Diag_Code_13_Poa,
      ICD_DIAG_14 as Icd_Diag_14,
      DIAG_CODE_14_POA as Diag_Code_14_Poa,
      ICD_DIAG_15 as Icd_Diag_15,
      DIAG_CODE_15_POA as Diag_Code_15_Poa,
      ICD_DIAG_16 as Icd_Diag_16,
      DIAG_CODE_16_POA as Diag_Code_16_Poa,
      ICD_DIAG_17 as Icd_Diag_17,
      DIAG_CODE_17_POA as Diag_Code_17_Poa,
      ICD_DIAG_18 as Icd_Diag_18,
      DIAG_CODE_18_POA as Diag_Code_18_Poa,
      ICD_DIAG_19 as Icd_Diag_19,
      DIAG_CODE_19_POA as Diag_Code_19_Poa,
      ICD_DIAG_20 as Icd_Diag_20,
      DIAG_CODE_20_POA as Diag_Code_20_Poa,
      ICD_DIAG_21 as Icd_Diag_21,
      DIAG_CODE_21_POA as Diag_Code_21_Poa,
      ICD_DIAG_22 as Icd_Diag_22,
      DIAG_CODE_22_POA as Diag_Code_22_Poa,
      ICD_DIAG_23 as Icd_Diag_23,
      DIAG_CODE_23_POA as Diag_Code_23_Poa,
      ICD_DIAG_24 as Icd_Diag_24,
      DIAG_CODE_24_POA as Diag_Code_24_Poa,
      ICD_DIAG_25 as Icd_Diag_25,
      DIAG_CODE_25_POA as Diag_Code_25_Poa,
      ICD_DIAG_26 as Icd_Diag_26,
      DIAG_CODE_26_POA as Diag_Code_26_Poa,
      ICD_DIAG_27 as Icd_Diag_27,
      DIAG_CODE_27_POA as Diag_Code_27_Poa,
      ICD_DIAG_28 as Icd_Diag_28,
      DIAG_CODE_28_POA as Diag_Code_28_Poa,
      ICD_DIAG_29 as Icd_Diag_29,
      DIAG_CODE_29_POA as Diag_Code_29_Poa,
      ICD_DIAG_30 as Icd_Diag_30,
      DIAG_CODE_30_POA as Diag_Code_30_Poa,
      ICD_DIAG_31 as Icd_Diag_31,
      DIAG_CODE_31_POA as Diag_Code_31_Poa,
      ICD_DIAG_32 as Icd_Diag_32,
      DIAG_CODE_32_POA as Diag_Code_32_Poa,
      ICD_DIAG_33 as Icd_Diag_33,
      DIAG_CODE_33_POA as Diag_Code_33_Poa,
      ICD_DIAG_34 as Icd_Diag_34,
      DIAG_CODE_34_POA as Diag_Code_34_Poa,
      FACILITY_TYPE as Facility_Type,
      CHARGE_AMOUNT as Charge_Amount,
      RENDERING_PROVIDER_NPI as Rendering_Provider_Npi,
      REFERRING_PROVIDER_NPI as Referring_Provider_Npi,
      REFERRAL_NUMBER as Referral_Number,
      RENDERING_PROVIDER_ID as Rendering_Provider_Id,
      REFERRING_PROVIDER_ID as Referring_Provider_Id,
      PTY_UNPROTECT_SSN(PAYTO_PROVIDER_TIN) AS Payto_Provider_Tin,
      RENDERING_PROVIDER_GROUP_ID as Rendering_Provider_Group_Id,
      SPECIALTY_CODE as Specialty_Code,
      SPECIALTY_NAME as Specialty_Name,
      AUTHORIZATION_NUMBER as Authorization_Number,
      TYPE_OF_ADMIT as Type_Of_Admit,
      ADMIT_SOURCE as Admit_Source,
      DISCHARGE_STATUS as Discharge_Status,
      CAPPED_INDICATOR as Capped_Indicator,
      ICD_PROC_1 as Icd_Proc_1,
      ICD_PROC_2 as Icd_Proc_2,
      ICD_PROC_3 as Icd_Proc_3,
      ICD_PROC_4 as Icd_Proc_4,
      ICD_PROC_5 as Icd_Proc_5,
      ICD_PROC_6 as Icd_Proc_6,
      E_CODE_1 as E_Code_1,
      E_CODE_2 as E_Code_2,
      E_CODE_3 as E_Code_3,
      ICD_CODE_TYPE as Icd_Code_Type
    FROM """ + medDbName + "." + targetMarket + "_" + domainName + """ WHERE (UPDATE_TYPE IN ('ORIGINAL', 'ADJUSTMENT', 'BACKOUT') AND CLAIM_NUMBER IN (""" + clmDtlCk + """)OR UPDATE_TYPE IN ('DELETE'))"""

  var filterDel_ind_1 = if(RunType == "Hist") "AND UPDATE_TYPE <> 'DELETE'" else "OR  UPDATE_TYPE ='DELETE'"

  val medclaim_header_historyDF_withoutdefault_query: String =
    """
    SELECT distinct
      TENANT_SK as Tenant_sk,
      SOURCE_SYSTEM_SK as Source_System_sk,
      SOURCE_SYSTEM as Source_System,
      CONTRACT as Contract,
      PBP as Pbp,
      ENTITY_MEMBER_ID as Entity_Member_Id,
      HEALTH_PLAN as Health_Plan,
      HP_MEMBER_ID as Hp_Member_Id,
      CLAIM_NUMBER as Claim_Number,
      CLAIM_STATUS as Claim_Status,
      CLAIM_TYPE as Claim_Type,

      CASE
      WHEN CLAIM_ADJUST_TYPE = 'DELETE' THEN CLAIM_ADJUST_TYPE
      WHEN UPDATE_TYPE = 'ADJUSTMENT' AND (CLAIM_ADJUST_TYPE <> 'BACKOUT' OR UPDATE_TYPE <> 'BACKOUT') THEN 'ADJUSTMENT'
        ELSE CLAIM_ADJUST_TYPE
      END as Claim_Adjust_Type,

      ORIG_CLAIM_NUMBER as Orig_Claim_Number,
      ADMIT_DATE as Admit_Date,
      DISCHARGE_DATE as Discharge_Date,
      STATEMENT_FROM_DATE as Statement_From_Date,
      STATEMENT_TO_DATE as Statement_To_Date,
      BILL_TYPE as Bill_Type,
      INPATIENT_FLAG as Inpatient_Flag,
      DRG_CODE as Drg_Code,
      ICD_DIAG_ADMIT as Icd_Diag_Admit,
      ICD_DIAG_1 as Icd_Diag_1,
      DIAG_CODE_1_POA as Diag_Code_1_Poa,
      ICD_DIAG_2 as Icd_Diag_2,
      DIAG_CODE_2_POA as Diag_Code_2_Poa,
      ICD_DIAG_3 as Icd_Diag_3,
      DIAG_CODE_3_POA as Diag_Code_3_Poa,
      ICD_DIAG_4 as Icd_Diag_4,
      DIAG_CODE_4_POA as Diag_Code_4_Poa,
      ICD_DIAG_5 as Icd_Diag_5,
      DIAG_CODE_5_POA as Diag_Code_5_Poa,
      ICD_DIAG_6 as Icd_Diag_6,
      DIAG_CODE_6_POA as Diag_Code_6_Poa,
      ICD_DIAG_7 as Icd_Diag_7,
      DIAG_CODE_7_POA as Diag_Code_7_Poa,
      ICD_DIAG_8 as Icd_Diag_8,
      DIAG_CODE_8_POA as Diag_Code_8_Poa,
      ICD_DIAG_9 as Icd_Diag_9,
      DIAG_CODE_9_POA as Diag_Code_9_Poa,
      ICD_DIAG_10 as Icd_Diag_10,
      DIAG_CODE_10_POA as Diag_Code_10_Poa,
      ICD_DIAG_11 as Icd_Diag_11,
      DIAG_CODE_11_POA as Diag_Code_11_Poa,
      ICD_DIAG_12 as Icd_Diag_12,
      DIAG_CODE_12_POA as Diag_Code_12_Poa,
      ICD_DIAG_13 as Icd_Diag_13,
      DIAG_CODE_13_POA as Diag_Code_13_Poa,
      ICD_DIAG_14 as Icd_Diag_14,
      DIAG_CODE_14_POA as Diag_Code_14_Poa,
      ICD_DIAG_15 as Icd_Diag_15,
      DIAG_CODE_15_POA as Diag_Code_15_Poa,
      ICD_DIAG_16 as Icd_Diag_16,
      DIAG_CODE_16_POA as Diag_Code_16_Poa,
      ICD_DIAG_17 as Icd_Diag_17,
      DIAG_CODE_17_POA as Diag_Code_17_Poa,
      ICD_DIAG_18 as Icd_Diag_18,
      DIAG_CODE_18_POA as Diag_Code_18_Poa,
      ICD_DIAG_19 as Icd_Diag_19,
      DIAG_CODE_19_POA as Diag_Code_19_Poa,
      ICD_DIAG_20 as Icd_Diag_20,
      DIAG_CODE_20_POA as Diag_Code_20_Poa,
      ICD_DIAG_21 as Icd_Diag_21,
      DIAG_CODE_21_POA as Diag_Code_21_Poa,
      ICD_DIAG_22 as Icd_Diag_22,
      DIAG_CODE_22_POA as Diag_Code_22_Poa,
      ICD_DIAG_23 as Icd_Diag_23,
      DIAG_CODE_23_POA as Diag_Code_23_Poa,
      ICD_DIAG_24 as Icd_Diag_24,
      DIAG_CODE_24_POA as Diag_Code_24_Poa,
      ICD_DIAG_25 as Icd_Diag_25,
      DIAG_CODE_25_POA as Diag_Code_25_Poa,
      ICD_DIAG_26 as Icd_Diag_26,
      DIAG_CODE_26_POA as Diag_Code_26_Poa,
      ICD_DIAG_27 as Icd_Diag_27,
      DIAG_CODE_27_POA as Diag_Code_27_Poa,
      ICD_DIAG_28 as Icd_Diag_28,
      DIAG_CODE_28_POA as Diag_Code_28_Poa,
      ICD_DIAG_29 as Icd_Diag_29,
      DIAG_CODE_29_POA as Diag_Code_29_Poa,
      ICD_DIAG_30 as Icd_Diag_30,
      DIAG_CODE_30_POA as Diag_Code_30_Poa,
      ICD_DIAG_31 as Icd_Diag_31,
      DIAG_CODE_31_POA as Diag_Code_31_Poa,
      ICD_DIAG_32 as Icd_Diag_32,
      DIAG_CODE_32_POA as Diag_Code_32_Poa,
      ICD_DIAG_33 as Icd_Diag_33,
      DIAG_CODE_33_POA as Diag_Code_33_Poa,
      ICD_DIAG_34 as Icd_Diag_34,
      DIAG_CODE_34_POA as Diag_Code_34_Poa,
      FACILITY_TYPE as Facility_Type,
      CHARGE_AMOUNT as Charge_Amount,
      RENDERING_PROVIDER_NPI as Rendering_Provider_Npi,
      REFERRING_PROVIDER_NPI as Referring_Provider_Npi,
      REFERRAL_NUMBER as Referral_Number,
      RENDERING_PROVIDER_ID as Rendering_Provider_Id,
      REFERRING_PROVIDER_ID as Referring_Provider_Id,
      PTY_UNPROTECT_SSN(PAYTO_PROVIDER_TIN) AS Payto_Provider_Tin,
      RENDERING_PROVIDER_GROUP_ID as Rendering_Provider_Group_Id,
      SPECIALTY_CODE as Specialty_Code,
      SPECIALTY_NAME as Specialty_Name,
      AUTHORIZATION_NUMBER as Authorization_Number,
      TYPE_OF_ADMIT as Type_Of_Admit,
      ADMIT_SOURCE as Admit_Source,
      DISCHARGE_STATUS as Discharge_Status,
      CAPPED_INDICATOR as Capped_Indicator,
      ICD_PROC_1 as Icd_Proc_1,
      ICD_PROC_2 as Icd_Proc_2,
      ICD_PROC_3 as Icd_Proc_3,
      ICD_PROC_4 as Icd_Proc_4,
      ICD_PROC_5 as Icd_Proc_5,
      ICD_PROC_6 as Icd_Proc_6,
      E_CODE_1 as E_Code_1,
      E_CODE_2 as E_Code_2,
      E_CODE_3 as E_Code_3,
      ICD_CODE_TYPE as Icd_Code_Type
     FROM """ + medDbName + "." + targetMarket + "_" + domainName + """ WHERE (UPDATE_TYPE IN ('ORIGINAL', 'ADJUSTMENT', 'BACKOUT') AND CLAIM_NUMBER IN ( """ + clmDtlCk + """))""" + filterDel_ind_1
  def runNotebook(): String = {


      println("Running CDOS " +domainName.capitalize+ " Notebook")
      cdosBase.setLastExtractDT()
      println("")
      val currentDT = cdosBase.getExtractDate_inscope(targetMarket,dbName)
      println("")
      val currentDate = currentDT.replace("-", "").substring(2, 8)

      println("CURRENT_DATE" + currentDate)
      println("NEW_CURRENT_DATE_TIMESTAMP" + currentDT)

      val SourceDF = cdosBaseMedclaims.getDataframeFromSnowflake(targetQuery, cdosBaseMedclaims.sfSchema)

      println(s"CdosMedclaimHeader 1 : ${java.time.LocalDateTime.now}")

      cdosBaseMedclaims.dropTableifExists(medclaimHeaderSourceTable)
      cdosBaseMedclaims.writeDataFrameToDeltaLake(SourceDF, medclaimHeaderSourceTable, "overWrite")
      val inscope_patient_sp_table = dbName + "_" + targetMarket + "_inscope_patient"
      var inscopeDf_sp = spark.read
    .format("snowflake")
    .option("sfUrl", sfOptions("sfUrl"))
    .option("sfDatabase", sfOptions("sfDatabase"))
    .option("sfWarehouse", sfOptions("sfWarehouse"))
    .option("sfUser", sfOptions("sfUser"))
    .option("pem_private_key", sfOptions("pem_private_key"))
    .option("dbtable", inscope_patient_sp_table)  
    .load
   
   inscopeDf_sp.write.format("delta").mode("overwrite").saveAsTable(temp_inscope_patient_table)

    val inscopeDF = spark.sql(
        """ SELECT CLH.* FROM """ + medDbName + "." + targetMarket + "_" + domainName + """_source CLH
    INNER JOIN """ + temp_inscope_patient_table + """ IP
    ON CLH.SOURCE_PATIENT_ID = IP.SOURCE_PATIENT_ID
    AND CLH.TENANT_SK = IP.TENANT_SK
    AND IP.DEL_IND = 0
    AND IP.PROJECTABBR = 'CDOS'
    AND CLH.SERV_FROM_DT <= IP.LASTELIGENDDATE """ )

      val medclaim_header_historyDF_temp_str = cdosBaseMedclaims.defaultTable_insertion(targetMarket.toUpperCase,domainName, inscopeDF)
      var medclaim_header_historyDF_temp: DataFrame = null
      if ("SDM2".equals(CdosConfigMapMedclaims.sdm_type)) {
        medclaim_header_historyDF_temp = medclaim_header_historyDF_temp_str
          .withColumn("Charge_Amount",col("Charge_Amount").cast("decimal(32, 2)"))

      }
      if ("SDM3".equals(CdosConfigMapMedclaims.sdm_type)) {
        medclaim_header_historyDF_temp = medclaim_header_historyDF_temp_str
          .withColumn("Charge_Amount",col("Charge_Amount").cast("string"))
      }

      println(s"CdosMedclaimHeader 2 : ${java.time.LocalDateTime.now}")

      val medclaim_header_historyDF_withouthash = medclaim_header_historyDF_temp.drop("HASH")

      val medclaimDetailDF_history_withouthash_withoutupdate_type = medclaim_header_historyDF_withouthash.drop("UPDATE_TYPE")

      def drop_hash(): DataFrame = {
        medclaim_header_historyDF_temp.drop("HASH")
      }

      def hash_function(): DataFrame = {
        medclaim_header_historyDF_withouthash.withColumn("HASH", hash(medclaimDetailDF_history_withouthash_withoutupdate_type.columns.map(col): _*))
      }

      def medclaim_header_historyDF(): DataFrame = {
        drop_hash()
        hash_function()
      }

      println(s"CdosMedclaimHeader 3 : ${java.time.LocalDateTime.now}")

      val deltaDF = DeltaTable
        .createIfNotExists()
        .tableName(medDbName + "." + targetMarket + "_" + domainName)
        .addColumns(medclaim_header_historyDF.schema)
        .property("delta.enableChangeDataFeed", "true")
        .execute

      println(s"CdosMedclaimHeader 4 : ${java.time.LocalDateTime.now}")

      val headerLatestVersion = spark.sql(s"""DESCRIBE HISTORY  """ + medDbName + "." + targetMarket  + "_medclaim_header" + """  """ ).orderBy(col("version").desc).first.getLong(0)
      val detailsLatestVersionDF = spark.sql("Select Latest_Detail_Version as version from " + medDbName + "." + targetMarket  + "_medclaim_details_version")
      val detailsLatestVersion = detailsLatestVersionDF.select("version").collect()(0)(0)

      println(s"CdosMedclaimHeader 5 : ${java.time.LocalDateTime.now}")

      //////////////////////////////////////////
      // Merge conditions for the delta table //
      //////////////////////////////////////////
      val updateConditions =
        """
          delta.SOURCE_SYSTEM = source.SOURCE_SYSTEM AND
          delta.TENANT_SK = source.TENANT_SK AND
          delta.CLAIM_NUMBER = source.CLAIM_NUMBER AND
          delta.HASH <> source.HASH
      """

      val insertConditions =
        """
          delta.SOURCE_SYSTEM = source.SOURCE_SYSTEM AND
          delta.TENANT_SK = source.TENANT_SK AND
          delta.CLAIM_NUMBER = source.CLAIM_NUMBER
      """
      // update any records where the hashes do not match.
      deltaDF
        .as("delta")
        .merge(medclaim_header_historyDF.as("source"), updateConditions)
        .whenMatched
        .updateAll
        .execute

      println(s"CdosMedclaimHeader 6 : ${java.time.LocalDateTime.now}")

      // add any new inserts to the table.
      deltaDF
        .as("delta")
        .merge(medclaim_header_historyDF.as("source"), insertConditions)
        .whenNotMatched
        .insertAll
        .execute

      def headerCurrentVersion() = {
        spark.sql(s"""DESCRIBE HISTORY """ + medDbName + "." + targetMarket + "_medclaim_header" + """  """).filter(col("operation") === "MERGE").orderBy(col("version").desc).first.getLong(0)
      }

      def detailsCurrentVersion() = {
        spark.sql(s"""DESCRIBE HISTORY """ + medDbName + "." + targetMarket + "_medclaim_details" + """  """).filter(col("operation") === "MERGE").orderBy(col("version").desc).first.getLong(0)
      }

      println(s"CdosMedclaimHeader 7 : ${java.time.LocalDateTime.now}")

      // Grab the max commit timestamp from medclaim details delta table to use for incremental logic
      def getExtractDate_medclaimdetails(targetMarket: String, dbName: String): String = {
        val targetMarketUC = targetMarket.toUpperCase()
        val sqlQuery = s"""select max(timestamp) from (DESCRIBE HISTORY """ + dbName + "." + targetMarket + "_medclaim_details" + """ ) where operation = 'MERGE' """
        val extractDt = spark.sql(sqlQuery).collect()(0)(0)
        extractDt.toString
      }

      def getExtractDate_medclaimheader(targetMarket: String, dbName: String): String = {
        val targetMarketUC = targetMarket.toUpperCase()
        val sqlQuery = s"""select max(timestamp) from (DESCRIBE HISTORY """ + dbName + "." + targetMarket + "_medclaim_header" + """) where operation = 'MERGE' """
        val extractDt = spark.sql(sqlQuery).collect()(0)(0)
        extractDt.toString
      }
      val max_DT_medclaims_details = getExtractDate_medclaimdetails(targetMarket, medDbName).substring(0, 10)
      val max_DT_medclaims_header = getExtractDate_medclaimheader(targetMarket, medDbName).substring(0, 10)

      println("")
      print(max_DT_medclaims_details)
      println("")
      print(max_DT_medclaims_header)

      

      var headerStartDate = cdosBase.lastExtractDT.substring(0, 19);
      println("headerStartDate"+headerStartDate)
      var detailsStartDate = sfCDCM.sql(String.format("SELECT PREVIOUS_LAST_EXTRACT_DT FROM " + dbName +"_" + targetMarket + "_last_extract_info WHERE FILE_NAME = 'medclaim_details'", targetMarket.toUpperCase())).select("PREVIOUS_LAST_EXTRACT_DT").collect().map(_.getTimestamp(0)).mkString("");
      var endDate = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC);

      println("detailsStartDate"+detailsStartDate)

      var headerInsMaxCommitDT = spark.sql(String.format("select max(_commit_timestamp) AS commit_timestamp  from table_changes('" +  medDbName +"." + targetMarket + "_medclaim_header', '" + max_DT_medclaims_header +"') where _change_type = 'insert'")).select("commit_timestamp").collect().map(_.getTimestamp(0)).mkString("");

      println("headerInsMaxcommit")
      var headerUpdMaxCommitDT = spark.sql(String.format("select max(_commit_timestamp) aS commit_timestamp from table_changes('" +  medDbName +"." + targetMarket + "_medclaim_header', '" + max_DT_medclaims_header +"') where _change_type = 'update_postimage'")).select("commit_timestamp").collect().map(_.getTimestamp(0)).mkString("");

      var detailInsMaxCommitDT = spark.sql(String.format("select max(_commit_timestamp) As commit_timestamp from table_changes('" +  medDbName +"." + targetMarket +  "_medclaim_details' , '" + max_DT_medclaims_details +"') where _change_type = 'insert'")).select("commit_timestamp").collect().map(_.getTimestamp(0)).mkString("");

      var detailUpdMaxCommitDT = spark.sql(String.format("select max(_commit_timestamp) as commit_timestamp from table_changes('" +  medDbName +"." + targetMarket +"_medclaim_details','" + max_DT_medclaims_details +"') where _change_type = 'update_postimage'")).select("commit_timestamp").collect().map(_.getTimestamp(0)).mkString("");


      // Show what dates we are seaching for:
      println(String.format("[Info]: Searching for header - records between startdate '%s' and the enddate '%s'", headerStartDate, endDate))
      println(String.format("[Info]: Searching for detail - records between startdate '%s' and the enddate '%s'", detailsStartDate, endDate))
      println(String.format("[Info]: Header Insert Max Commit Time '%s' and Update Max Commit Time '%s'", headerInsMaxCommitDT, headerUpdMaxCommitDT))
      println(String.format("[Info]: Detail Insert Max Commit Time '%s' and Update Max Commit Time '%s'", detailInsMaxCommitDT, detailUpdMaxCommitDT))
      
      println(s"CdosMedclaimHeader 8 : ${java.time.LocalDateTime.now}")

      // Queries to handle inserts and updates for both the header and detail notebooks.
      val headerInserts = """
    SELECT SOURCE_SYSTEM, TENANT_SK, CLAIM_NUMBER FROM table_changes('"""+ medDbName + "." + targetMarket + """_medclaim_header' , """ + headerLatestVersion + """,""" + headerCurrentVersion() + """)
    WHERE _change_type = 'insert' and _commit_timestamp = '""" + headerInsMaxCommitDT + """'
    AND HDR.SOURCE_SYSTEM = SOURCE_SYSTEM
    AND HDR.TENANT_SK = TENANT_SK
    AND HDR.CLAIM_NUMBER = CLAIM_NUMBER
    """;
 
      val headerUpdates = """
    SELECT SOURCE_SYSTEM, TENANT_SK, CLAIM_NUMBER FROM table_changes('""" + medDbName + "." + targetMarket + """_medclaim_header' ,  """ + headerLatestVersion + """,""" + headerCurrentVersion() + """)
    WHERE  _change_type = 'update_postimage' and _commit_timestamp = '""" + headerUpdMaxCommitDT + """'
    AND HDR.SOURCE_SYSTEM = SOURCE_SYSTEM
    AND HDR.TENANT_SK = TENANT_SK
    AND HDR.CLAIM_NUMBER = CLAIM_NUMBER
    """;

      val detailInserts = """
    SELECT SOURCE_SYSTEM_SK, TENANT_SK, CLAIM_NUMBER FROM table_changes('"""+ medDbName + "." + targetMarket + """_medclaim_details' , """ + detailsLatestVersion + """,""" + detailsCurrentVersion() + """)
    WHERE _change_type = 'insert' and _commit_timestamp = '""" + detailInsMaxCommitDT + """'
    AND HDR.SOURCE_SYSTEM_SK = SOURCE_SYSTEM_SK
    AND HDR.TENANT_SK = TENANT_SK
    AND HDR.CLAIM_NUMBER = CLAIM_NUMBER
    """;

      val detailUpdates = """
    SELECT SOURCE_SYSTEM_SK, TENANT_SK, CLAIM_NUMBER FROM table_changes('""" + medDbName + "." + targetMarket + """_medclaim_details' ,""" + detailsLatestVersion + """,""" + detailsCurrentVersion() + """)
    WHERE _change_type = 'update_postimage' and _commit_timestamp = '""" + detailUpdMaxCommitDT + """'
    AND HDR.SOURCE_SYSTEM_SK = SOURCE_SYSTEM_SK
    AND HDR.TENANT_SK = TENANT_SK
    AND HDR.CLAIM_NUMBER = CLAIM_NUMBER
    """;
      // Table updates, give priority to inserts over updates.

      println(s"CdosMedclaimHeader 9 : ${java.time.LocalDateTime.now}")

      val update_variable = "UPDATE "
      spark.sql(update_variable + medDbName + "." + targetMarket +  "_medclaim_header as HDR SET UPDATE_TYPE = 'ORIGINAL' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE <>'DELETE' AND EXISTS (" + headerInserts + ")");

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'ORIGINAL' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE <>'DELETE' AND EXISTS (" + detailInserts + ")");

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'ADJUSTMENT' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE NOT IN ('DELETE', 'BACKOUT') AND  EXISTS (" + headerUpdates + ")");

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'ADJUSTMENT' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE NOT IN ('DELETE', 'BACKOUT') AND EXISTS (" + detailUpdates + ")");


      spark.sql(update_variable + medDbName + "." + targetMarket +  "_medclaim_header as HDR SET UPDATE_TYPE = 'DELETE' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE ='DELETE' AND EXISTS (" + headerInserts + ")");

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'DELETE' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE ='DELETE' AND EXISTS (" + headerUpdates + ")");

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'BACKOUT' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE ='BACKOUT' AND EXISTS (" + headerInserts + ")")
  
      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'BACKOUT' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE ='BACKOUT' AND EXISTS (" + headerUpdates + ")")

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'BACKOUT' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE ='BACKOUT' AND EXISTS (" + detailUpdates + ")")

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header as HDR SET UPDATE_TYPE = 'BACKOUT' WHERE UPDATE_TYPE ='' AND CLAIM_ADJUST_TYPE ='BACKOUT' AND EXISTS (" + detailInserts + ")")

      println(s"CdosMedclaimHeader 10 : ${java.time.LocalDateTime.now}")

      val select_variable = "SELECT * FROM "

      println(s"CdosMedclaimHeader 11 : ${java.time.LocalDateTime.now}")

      val updatesQuery = "(select CLAIM_NUMBER from table_changes('" + medDbName + "." + targetMarket + "_medclaim_details' ,  '" + max_DT_medclaims_details + "') where (_change_type = 'insert' and _commit_timestamp = (select max(_commit_timestamp) from table_changes('" + medDbName + "." + targetMarket + "_medclaim_details', '" + max_DT_medclaims_details + "') where _change_type = 'insert')) or (_change_type = 'update_postimage' and _commit_timestamp = (select max(_commit_timestamp) from table_changes('" + medDbName + "." + targetMarket + "_medclaim_details', '" + max_DT_medclaims_details + "') where _change_type = 'update_postimage'))) union (SELECT CLAIM_NUMBER FROM table_changes('" + medDbName + "." + targetMarket + "_medclaim_header', " + headerLatestVersion + "," + headerCurrentVersion + ") WHERE _change_type = 'insert' and _commit_timestamp = '" + headerInsMaxCommitDT + "') union (SELECT CLAIM_NUMBER FROM table_changes('" + medDbName + "." + targetMarket + "_medclaim_header', " + headerLatestVersion + ", " + headerCurrentVersion + ") WHERE  _change_type = 'update_postimage' and _commit_timestamp = '" + headerUpdMaxCommitDT + "')"

      println(s"CdosMedclaimHeader 12 : ${java.time.LocalDateTime.now}")

      val clmHdrCk = "select distinct CLAIM_NUMBER FROM " + medDbName + "." + targetMarket + "_medclaim_header WHERE UPDATE_TYPE IN ('ORIGINAL', 'ADJUSTMENT', 'BACKOUT')"

      val incrementalEvents = spark.sql(select_variable + medDbName + "." + targetMarket + "_medclaim_details WHERE CLAIM_NUMBER IN (" + updatesQuery + ") AND CLAIM_NUMBER IN (" + clmHdrCk + ")")

      val medclaim_details_historyDF =
        incrementalEvents.select(
          "TENANT_SK",
          "SOURCE_SYSTEM_SK",
          "SOURCE_SYSTEM",
          "CLAIM_NUMBER",
          "SERVICE_LINE",
          "REVENUE_CODE",
          "FACILITY_TYPE_CODE",
          "PROCEDURE_CODE",
          "MODIFIER_1",
          "MODIFIER_2",
          "MODIFIER_3",
          "MODIFIER_4",
          "EMERGENCY_FLAG",
          "NDC_CODE",
          "HOSPITAL_RELATED_FLAG",
          "OUTSIDE_LABS_FLAG",
          "CAPPED_INDICATOR",
          "PAID_AMOUNT",
          "BILLED_AMOUNT",
          "ALLOWED_AMOUNT",
          "UNIT_COUNT",
          "SERVICE_FROM_DATE",
          "SERVICE_TO_DATE",
          "BILLED_DATE",
          "PROCESSED_DATE",
          "PAID_DATE"
        ).where("Del_ind = 0")

      println(s"CdosMedclaimHeader 13 : ${java.time.LocalDateTime.now}")

      val tmForFileNameDtl = cdosBase.filename_extraction(tenant,targetMarket.toUpperCase())
      
      println(s"CdosMedclaimHeader 14 : ${java.time.LocalDateTime.now}")

      val tenantList = tenant.split(",").map(_.trim)

      val medclaim_details_historyDF_updated = if (tenantList.length > 1) {
        medclaim_details_historyDF.withColumn("TENANT_SK", lit(0)).withColumn("SOURCE_SYSTEM_SK", lit(0)).dropDuplicates()
      } else {
        medclaim_details_historyDF
      }

      val medclaim_details_DeltaDF = cdosBaseMedclaims.dropcolumnsfromDataframe(medclaim_details_historyDF_updated,Seq("TENANT_SK", "SOURCE_SYSTEM_SK"))

      var lastExtractDate = ""
      try {
        lastExtractDate = cdosBase.getLastExtractDate()
      } catch {
        case e: StringIndexOutOfBoundsException => lastExtractDate = cdosBase.lastExtractDT.toString.replace("-", "").substring(0, 8)
      }

      println(s"CdosMedclaimHeader 15 : ${java.time.LocalDateTime.now}")

      var reportNamePrefix_D = tmForFileNameDtl + "_" + RunType + "_" + "MedClaim_D" + "_" + lastExtractDate + "_" + currentDate
      val reportName = reportNamePrefix_D
      println("reportName: "+reportName)
      val valReportName = reportName + ".txt"
      val tmForFileName = cdosBase.filename_extraction(tenant,targetMarket.toUpperCase())

      val reportNamePrefix_H = tmForFileName + "_" + RunType + "_" + "MedClaim_H" + "_" + lastExtractDate + "_" + currentDate

      println(s"CdosMedclaimHeader 16 : ${java.time.LocalDateTime.now}")

      cdosBase.archiveExtracts(tmForFileName + "_Hist_" + "MedClaim_H" + "_")

      println(s"CdosMedclaimHeader 17 : ${java.time.LocalDateTime.now}")

      cdosBase.archiveExtracts(tmForFileName + "_" + RunType + "_" + "MedClaim_H" + "_")

      println(s"CdosMedclaimHeader 18 : ${java.time.LocalDateTime.now}")

      cdosBaseMedclaims.loadClaimDetailsExtractsHistoryTableMedclaims(valReportName, medclaim_details_historyDF_updated)

      println(s"CdosMedclaimHeader 19 : ${java.time.LocalDateTime.now}")

var medclaim_details_hist_df = spark.read
        .format("snowflake")
        .options(sfOptions)
        .option("dbtable", detailshistoryTable)
        .load()
medclaim_details_hist_df.write.format("delta").mode("overwrite").saveAsTable(temp_details_history_table)

      println(s"CdosMedclaimHeader 20 : ${java.time.LocalDateTime.now}")




      cdosBase.archiveExtracts(tmForFileNameDtl + "_Hist_" + "MedClaim_D" + "_")

      println(s"CdosMedclaimHeader 21 : ${java.time.LocalDateTime.now}")

      cdosBase.archiveExtracts(tmForFileNameDtl + "_" + RunType + "_" + "MedClaim_D" + "_")

      println(s"CdosMedclaimHeader 22 : ${java.time.LocalDateTime.now}")

      var medclaim_details_DeltaDF_sp_table = dbName + targetMarket+"_medclaim_details_DeltaDF_sp_" 
      cdosBaseMedclaims.createSnowflakeTable(medclaim_details_DeltaDF_sp_table, medclaim_details_DeltaDF)
      var medclaim_details_DeltaDF_sp_df = cdosBase.getDataFromSnowflakeTable(medclaim_details_DeltaDF_sp_table)

      println(s"CdosMedclaimHeader 23 : ${java.time.LocalDateTime.now}")

      cdosBase.generateExtractAndUpload(reportNamePrefix_D, medclaim_details_DeltaDF_sp_df)
      cdosBase.updatePreviousExtractDate(cdosBase.lastExtractDT, targetMarket, "medclaim_details")

      println(s"CdosMedclaimHeader 24 : ${java.time.LocalDateTime.now}")

      cdosBase.updateLastExtractDate_exceptinscope_medclaim_details()

      println(s"CdosMedclaimHeader 25 : ${java.time.LocalDateTime.now}")

      medclaim_details_historyDF.createOrReplaceTempView("medclaim_details_historyDF_withoutdefault_Del_Ind0")
      println("medclaim_details_historyDF_withoutdefault_Del_Ind0 view created")
      val medclaim_details_del_ind_1 = incrementalEvents.select(
        "TENANT_SK",
        "SOURCE_SYSTEM_SK",
        "SOURCE_SYSTEM",
        "CLAIM_NUMBER",
        "SERVICE_LINE",
        "REVENUE_CODE",
        "FACILITY_TYPE_CODE",
        "PROCEDURE_CODE",
        "MODIFIER_1",
        "MODIFIER_2",
        "MODIFIER_3",
        "MODIFIER_4",
        "EMERGENCY_FLAG",
        "NDC_CODE",
        "HOSPITAL_RELATED_FLAG",
        "OUTSIDE_LABS_FLAG",
        "CAPPED_INDICATOR",
        "PAID_AMOUNT",
        "BILLED_AMOUNT",
        "ALLOWED_AMOUNT",
        "UNIT_COUNT",
        "SERVICE_FROM_DATE",
        "SERVICE_TO_DATE",
        "BILLED_DATE",
        "PROCESSED_DATE",
        "PAID_DATE"
      ).where("del_ind = 1")

      println(s"CdosMedclaimHeader 26 : ${java.time.LocalDateTime.now}")

      medclaim_details_del_ind_1.createOrReplaceTempView("medclaim_details_historyDF_withoutdefault_Del_Ind1")
      //----------------------------------Start Medclaim Header--------------------------------------------
      println("STARTING MEDCLAIMS HEADER")
      val medclaim_header_historyDF_withoutdefault_both_del_ind_query = cdosBaseMedclaims.getDataframeFromDeltaLake(medclaim_header_historyDF_withoutdefault_both_del_ind)

      println(s"CdosMedclaimHeader 27 : ${java.time.LocalDateTime.now}")

      val medclaim_header_historyDF_withoutdefault = spark.sql(medclaim_header_historyDF_withoutdefault_query)

      println(s"CdosMedclaimHeader 28 : ${java.time.LocalDateTime.now}")

      var medclaim_header_historyDF_both_del_ind: DataFrame = null
      if (RunType == "Hist" && "SDM2".equals(CdosConfigMapMedclaims.sdm_type)) {
        val medclaim_header_historyDF_both_del_ind_str = cdosBaseMedclaims.defaultTable_insertion(targetMarket.toUpperCase, domainName, medclaim_header_historyDF_withoutdefault_both_del_ind_query)
        medclaim_header_historyDF_both_del_ind=medclaim_header_historyDF_both_del_ind_str
          .withColumn("Charge_Amount",col("Charge_Amount").cast("decimal(32,2)"))
      }
      if (RunType == "Hist" && "SDM3".equals(CdosConfigMapMedclaims.sdm_type)) {
        val medclaim_header_historyDF_both_del_ind_str = cdosBaseMedclaims.defaultTable_insertion(targetMarket.toUpperCase, domainName, medclaim_header_historyDF_withoutdefault_both_del_ind_query)
        medclaim_header_historyDF_both_del_ind=medclaim_header_historyDF_both_del_ind_str
          .withColumn("Charge_Amount",col("Charge_Amount").cast("string"))
      }

      println(s"CdosMedclaimHeader 29 : ${java.time.LocalDateTime.now}")

      val medclaim_header_historyDF_1_str = cdosBaseMedclaims.defaultTable_insertion(targetMarket.toUpperCase, domainName, medclaim_header_historyDF_withoutdefault)
      var medclaim_header_historyDF_1: DataFrame = null
      if ("SDM2".equals(CdosConfigMapMedclaims.sdm_type)) {
        medclaim_header_historyDF_1 =medclaim_header_historyDF_1_str
          .withColumn("Charge_Amount",col("Charge_Amount").cast("decimal(32,2)"))
      }
      if ("SDM3".equals(CdosConfigMapMedclaims.sdm_type)) {
        medclaim_header_historyDF_1 =medclaim_header_historyDF_1_str
          .withColumn("Charge_Amount",col("Charge_Amount").cast("string"))
      }

      println(s"CdosMedclaimHeader 30 : ${java.time.LocalDateTime.now}")

      val tenantList_header = tenant.split(",").map(_.trim)

      var medclaim_header_historyDF_both_del_ind_updated: DataFrame = null

      if (RunType == "Hist") {
        medclaim_header_historyDF_both_del_ind_updated = if (tenantList_header.length > 1) {
          medclaim_header_historyDF_both_del_ind.withColumn("TENANT_SK", lit(0)).withColumn("SOURCE_SYSTEM_SK", lit(0)).dropDuplicates()
        } else {
          medclaim_header_historyDF_both_del_ind
        }
      }

      val medclaim_header_historyDF_1_updated = if (tenantList_header.length > 1) {
        medclaim_header_historyDF_1.withColumn("TENANT_SK", lit(0)).withColumn("SOURCE_SYSTEM_SK", lit(0)).dropDuplicates()
      } else {
        medclaim_header_historyDF_1
      }

      val medclaim_header_Delta = medclaim_header_historyDF_1_updated.drop("Tenant_sk", "Source_System_sk")

      try {
        lastExtractDate = cdosBase.getLastExtractDate()
        println("lastExtractDate: "+lastExtractDate)
      } catch {
        case e: StringIndexOutOfBoundsException => lastExtractDate = cdosBase.lastExtractDT.toString.replace("-", "").substring(2, 8)
      }

      var medclaim_header_Delta_sp_table = dbName + targetMarket +  "_medclaim_header_Delta_sp"
      cdosBaseMedclaims.createSnowflakeTable(medclaim_header_Delta_sp_table, medclaim_header_Delta)

      var medclaim_header_Delta_sp_df = cdosBase.getDataFromSnowflakeTable(medclaim_header_Delta_sp_table)

      cdosBase.generateExtractAndUpload(reportNamePrefix_H, medclaim_header_Delta_sp_df)
      if (RunType == "Hist") {
        cdosBaseMedclaims.loadExtractsHistoryTableMedclaims(reportNamePrefix_H + ".txt", medclaim_header_historyDF_both_del_ind_updated)
      }
      else {
        cdosBaseMedclaims.loadExtractsHistoryTableMedclaims(reportNamePrefix_H + ".txt", medclaim_header_historyDF_1_updated)
        println("loadExtractsHistoryTableMedclaims runtype header")
      }

      spark.sql(update_variable + medDbName + "." + targetMarket + "_medclaim_header SET UPDATE_TYPE = '' WHERE TRIM(UPDATE_TYPE) in ('ORIGINAL', 'ADJUSTMENT','DELETE','BACKOUT')")
      cdosBase.updatePreviousExtractDate(cdosBase.lastExtractDT, targetMarket, domainName)
      cdosBase.updateLastExtractDT_exceptinscope()
      spark.sql("drop table if exists "+temp_details_history_table)
      spark.sql("drop table if exists "+temp_inscope_patient_table)

      cdosBase.dropSnowflakeTable(medclaim_header_Delta_sp_table)
      cdosBase.dropSnowflakeTable(medclaim_details_DeltaDF_sp_table)
      val status = "Success"
      return domainName + status

  }
}
