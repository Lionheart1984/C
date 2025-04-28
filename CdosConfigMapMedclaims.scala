// Databricks notebook source
// %run ../../SnowparkConnection

// COMMAND ----------

import org.apache.spark.sql.functions._
//import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.Session

import scala.List

class CdosConfigMapMedclaims(var notebookParams: scala.collection.mutable.Map[String,String], sfCDCM: Session) extends SparkSessionWrapper {

  import spark.implicits._
  val cdosBaseMedclaims = new CdosBaseMedclaims(notebookParams)
  val cdosBase = new CdosBase(notebookParams, sfCDCM)
  val lookbackPeriod = notebookParams("lookbackPeriod")
  val domainName = notebookParams("domainName")
  val targetMarket = notebookParams("targetMarket")
  val consumer = notebookParams("consumer")
  val sourceSystem = notebookParams("sourceSystem")
  val tenant = notebookParams("tenant")
  val TENANT_NUMBER = "(" + tenant + ")"
  val useSparkLastExtractForTimeTravel=notebookParams("useSparkLastExtractForTimeTravel")
  var timeTravel=""
  if (useSparkLastExtractForTimeTravel == "true")
    {timeTravel = cdosBase.getTimeTravel(domainName, targetMarket)}

  val wellmed = "WELLMED"
  val arcadia = "ARCADIA"
  val sdm2type = cdosBaseMedclaims.sdm_extraction("NY")
  val sdm3type = cdosBaseMedclaims.sdm_extraction("CT")
  val eid_applied = cdosBaseMedclaims.eid_applied(cdosBaseMedclaims.targetMarket)
  val sdm2_markets = cdosBaseMedclaims.market_extraction("SDM2").mkString("(\"", "\",\"", "\")")
  val sdm3_markets = cdosBaseMedclaims.market_extraction("SDM3").mkString("(\"", "\",\"", "\")")

  val RENPG_variable = "RENPG."

  var DF_conf= Seq(
    ("MEDCLAIM_DETAILS", sdm2type, "N", "NY", arcadia,"",""),
    ("MEDCLAIM_DETAILS", sdm2type, "N", sdm2_markets, "","",""),
    ("MEDCLAIM_DETAILS", sdm3type, "N", sdm3_markets, "","",""),
    ("MEDCLAIM_DETAILS", sdm3type, "N", "CT", wellmed,"",""),
    ("MEDCLAIM_HEADER", sdm2type, "N", "NY", arcadia,RENPG_variable,""),
    ("MEDCLAIM_HEADER", sdm2type, "N", sdm2_markets, "",RENPG_variable,""),
    ("MEDCLAIM_HEADER", sdm3type, "N", sdm3_markets, "",RENPG_variable,""),
    ("MEDCLAIM_HEADER", sdm3type, "N", "CT", wellmed,RENPG_variable,""),
  ).toDF("Domain", "SDM", "Eid_Applied", "Markets", "Consumer","PrefixAlias","ConfigName")

  def config_extraction(Domain: String, market: String, consumer: String): (List[String], String,String, String) = {
    val df = DF_conf.filter(col("Domain") === Domain).filter(col("Markets").contains(market)).
      select(col("ConfigName"))
    val config_var_list: List[String] = if (df.count > 0) {
      df.distinct.collect().map(_.getString(0)).toList
    }
    else List("")
    println(config_var_list)
    val df1 = DF_conf.filter(col("Domain") === Domain).filter(col("Markets").contains(market))
      .select(col("PrefixAlias"))
    val pgi_prefix: String = if (df1.count > 0) {
      df1.rdd.map(x => x(0).toString).collect()(0)
    }
    else null
    println(pgi_prefix)
    val df2 = DF_conf.filter(col("Domain") === Domain).filter(col("Markets").contains(market))
      .select(col("Eid_Applied"))
    val eid: String = if (df2.count > 0) {
      df2.rdd.map(x => x(0)).collect()(0).toString
    }
    else null
    println(eid)
    val df3 = DF_conf.filter(col("Domain") === Domain).filter(col("Markets").contains(market))
      .select(col("SDM"))
    val sdm_type: String = if (df3.count > 0) {
      df3.rdd.map(x => x(0)).collect()(0).toString
    }
    else null
    println(sdm_type)
    (config_var_list, pgi_prefix, eid, sdm_type)
  }

  val(config_var_list,pgi_prefix,eid, sdm_type)=config_extraction(domainName.toUpperCase(),targetMarket.toUpperCase(),consumer)
  println(s"config_var_list,pgi_prefix,pgi_source_system,sdm_type :$config_var_list,$pgi_prefix,$eid,$sdm_type")

}// End of Class
