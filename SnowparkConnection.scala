// Databricks notebook source
// Import the Snowpark library from Maven.
import com.snowflake.snowpark.{DataFrame => SpDataFrame}
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Session._
import com.snowflake.snowpark.SaveMode.Overwrite
import com.snowflake.snowpark.types.{StringType, StructField, StructType}

// COMMAND ----------

import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime}
var runid = java.time.ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC).toString().take(19)
var cluster = spark.conf.get("spark.databricks.clusterUsageTags.clusterId") 
var queryTag = "CdosSnowpark|" + cluster //+ "|" + runid

// COMMAND ----------

def createNbParameters() {
  dbutils.widgets.dropdown("databricksEnv", "PRD", Seq("DEV", "STG", "PRD"), "Databricks Env.")
  dbutils.widgets.dropdown("snowflakeEnv", "PRD", Seq("DEV", "STG", "SIT", "PRD"), "SnowflakeEnvironment.")
  dbutils.widgets.text("sfSchema","CDCM_REFINED","snowflake schema")
  dbutils.widgets.text("sfWarehouse", "OCDP_STG_DATA_LOAD_32_WH", "Warehouse")
}
createNbParameters()

// COMMAND ----------

def setNbParameters() {
  var databricksEnv = dbutils.widgets.get("databricksEnv")
  var sfEnv = dbutils.widgets.get("snowflakeEnv")
  var sfWarehouse = dbutils.widgets.get("sfWarehouse")
  var sfSchema = dbutils.widgets.get("sfSchema")
  var sfDatabase = "OCDP_" + sfEnv + "_CDCM_DB"
}
setNbParameters()

// COMMAND ----------

// Get the parameter values.
var sfEnv = dbutils.widgets.get("snowflakeEnv")
var sfWarehouse = dbutils.widgets.get("sfWarehouse")
var sfSchema = dbutils.widgets.get("sfSchema")
var sfDatabase = "OCDP_" + sfEnv + "_CDCM_DB"
var databricksEnv = dbutils.widgets.get("databricksEnv")
val secretScopeName = "ocdp-" + databricksEnv.toLowerCase();
val sfUrl = dbutils.secrets.get(secretScopeName, s"snowflake${sfEnv.capitalize}-sfUrl")
val sfUser = dbutils.secrets.get(secretScopeName, s"snowflake${sfEnv.capitalize}-sfUser")
val pem_private_key = dbutils.secrets.get(secretScopeName, s"snowflake${sfEnv.capitalize}-pem-private-key")

val sfOptions_CDCM = Map(
  "URL" -> sfUrl,
  "USER" -> sfUser,
  "PRIVATEKEY" -> pem_private_key,
  "WAREHOUSE" -> sfWarehouse,
  "DB" -> sfDatabase,
  "sfSchema" -> sfSchema,
  "QUERY_TAG" -> queryTag
)

val sfCDCM = Session.builder.configs(sfOptions_CDCM).create
sfCDCM.sql("USE SCHEMA CDCM_REFINED").collect()
sfCDCM.sql("alter session set query_tag='" + queryTag + "'").collect()

