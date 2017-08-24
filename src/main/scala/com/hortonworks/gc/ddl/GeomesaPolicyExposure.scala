package com.hortonworks.gc.ddl

import java.io.IOException
import java.util
import java.util.Collections

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.spark.GeoMesaSpark
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
  * Geomesa ingest for Policy Exposure
  */
object GeomesaPolicyExposure {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "policy_exposure_1M")

  val featureName = "policyexposureevent"

  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long",
    "peril_id:java.lang.Long",
    "policy_id:java.lang.Long",
    "account_id:java.lang.Long",
    "account_num:java.lang.Long",
    "policy_status:String",
    "pol_lob:String",
    "policy_name:String",
    "policy_num:java.lang.Long",
    "incept_date:String",
    "expire_date:String",
    "deduct_type:String",
    "limit_type:String",
    "prorata:String",
    "undercover:String",
    "premium:String",
    "policy_deduct:String",
    "policy_bl_deduct:String",
    "min_deduct:String",
    "max_deduct:String",
    "policy_limit:String",
    "policy_bl_grosslimit:java.lang.Double",
    "cov1_limit:java.lang.Double",
    "cov1_deduct:java.lang.Double",
    "cov2_limit:java.lang.Double",
    "cov2_deduct:java.lang.Double",
    "cov3_limit:java.lang.Double",
    "cov3_deduct:java.lang.Double",
    "cov4_limit:java.lang.Double",
    "cov4_deduct:java.lang.Double",
    "cov5_limit:java.lang.Double",
    "cov5_deduct:java.lang.Double",
    "cov6_limit:java.lang.Double",
    "cov6_deduct:java.lang.Double",
    "agg_deduct:java.lang.Double",
    "cmb_limit:java.lang.Double",
    "cmbinded_deduct:java.lang.Double",
    "sub_limit:java.lang.Double",
    "sub_deduct:java.lang.Double",
    "p_udf_met1:java.lang.Double",
    "p_udf_met2:java.lang.Double",
    "p_udf_met3:java.lang.Double",
    "p_udf_met4:java.lang.Double",
    "p_udf_met5:java.lang.Double",
    "p_udf_met6:java.lang.Double",
    "p_udf_met7:java.lang.Double",
    "p_udf_met8:java.lang.Double",
    "p_udf_met9:java.lang.Double",
    "p_udf_met10:java.lang.Double",
    "p_udf_met11:java.lang.Double",
    "p_udf_met12:java.lang.Double",
    "p_udf_met13:java.lang.Double",
    "p_udf_met14:java.lang.Double",
    "p_udf_met15:java.lang.Double",
    "p_udf_attr1:java.lang.Double",
    "p_udf_attr2:java.lang.Double",
    "p_udf_attr3:java.lang.Double",
    "p_udf_attr4:java.lang.Double",
    "p_udf_attr5:java.lang.Double",
    "p_udf_attr6:java.lang.Double",
    "p_udf_attr7:java.lang.Double",
    "p_udf_attr8:java.lang.Double",
    "p_udf_attr9:java.lang.Double",
    "p_udf_attr10:java.lang.Double",
    "p_udf_attr11:java.lang.Double",
    "p_udf_attr12:java.lang.Double",
    "p_udf_attr13:java.lang.Double",
    "p_udf_attr14:java.lang.Double",
    "p_udf_attr15:java.lang.Double",
    "policy_comments:String"
  )

  val featureType: SimpleFeatureType =
    buildGeomesaPolicyExposureEventFeatureType(featureName, attributes)

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore].createSchema(featureType)

  @throws(classOf[SchemaException])
  def buildGeomesaPolicyExposureEventFeatureType(
      featureName: String,
      attributes: util.ArrayList[String]): SimpleFeatureType = {
    val name = featureName
    val spec = Joiner.on(",").join(attributes)
    val featureType = DataUtilities.createType(name, spec)
    featureType.getUserData.put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "SQLDATE")
    featureType
  }

  def main(args: Array[String]) {
    println(""" DDL Policy Exposure completed ...""")
  }
}
