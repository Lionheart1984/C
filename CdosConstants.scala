// Databricks notebook source
object CdosConstants {

  val cdosInscopePatientDomainName:String = "inscope_patient"
  val cdosEligibilityDomainName:String = "eligibility"
  val cdosEmpiDomainName:String = "empi"
  val cdosLocationDomainName:String = "location"
  val cdosProviderGroupDomainName:String = "provider_group"
  val cdosProviderDomainName:String = "provider"
  val cdosPatientDomainName:String = "patient"
  val cdosPatientHicnDomainName:String = "incr_mbrhicnhistory"
  val cdosPodDomainName:String = "pod"
  val cdosMedclaimHeaderDomainName:String = "medclaim_header"
  val cdosMedclaimDetailsDomainName:String = "medclaim_details"
  val cdosLabResultDomainName:String = "lab_result"
  val cdosPharmacyDomainName:String = "pharmacy"

  val sdm2_mtnwest_markets = "id|co|nm"
  val sdm3_mtnwest_markets = "ut|az|nv"
  val mtnwest_ex_co_markets = "id|nv|nm|az|ut"
  val co_market = "co"
  val sdm2_midwest_markets = "ks"
  val cali_markets = "beaver|hcpapplecare"

  val cdosEligibilityMaterialized = "ELIGIBILITY_MATERIALIZED "
  val cdosMedclaimDetailsMaterialized = "CLAIM_DETAIL_MATERIALIZED "
  val cdosMedclaimHeaderMaterialized = "CLAIM_HEADER_MATERIALIZED "
  val cdosProcedureClaimMaterialized = "CLAIM_PROCEDURE_MATERIALIZED "
  val cdosPharmacyClaimMaterialized = "PHARMACY_CLAIM_MATERIALIZED "

}
