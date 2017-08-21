package com.hortonworks.gc.ingest

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
object GeomesaHbaseWritePolicyExposure {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "policy_exposure_1M")

  var ID_COL_IDX = 0
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "policyexposureevent"
  val ingestFile =
    "hdfs:///tmp/geospatial/policy_exposure_1M/policy_exposure_1M.csv"

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


  /**
    * This is the method to create the Simple feature.
    * @param value
    * @return
    */
  def createSimpleFeature(value: String): SimpleFeature = {

    val attributes: Array[String] = value.toString.split("\\|", -1)

    val simpleFeature: SimpleFeature =
      featureBuilder.buildFeature(attributes(ID_COL_IDX))
    // be sure to tell GeoTools explicitly that you want to use the ID you provided
    simpleFeature.getUserData
      .put(Hints.USE_PROVIDED_FID, java.lang.Boolean.FALSE)

    var i: Int = 0
    while (i < attributes.length) {
      simpleFeature.setAttribute(i, attributes(i))
      i += 1
    }

    simpleFeature
  }

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

    val conf = new SparkConf()
    //conf.setMaster("local[3]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator",
             "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")

    // Create Schema

    val sc = new SparkContext(conf.setAppName("Geomesa Policy Exposure Event Ingest"))

    val distDataRDD = sc.textFile(ingestFile)

    val processedRDD: RDD[SimpleFeature] = distDataRDD.mapPartitions {
      valueIterator =>
        if (valueIterator.isEmpty) {
          Collections.emptyIterator
        }

        //  setup code for SimpleFeatureBuilder
        try {
          val featureType: SimpleFeatureType =
            buildGeomesaPolicyExposureEventFeatureType(featureName, attributes)
          featureBuilder = new SimpleFeatureBuilder(featureType)
        } catch {
          case e: Exception => {
            throw new IOException("Error setting up feature type", e)
          }
        }

        valueIterator.map { s =>
          // Processing as before to build the SimpleFeatureType
          val simpleFeature = createSimpleFeature(s)
          if (!valueIterator.hasNext) {
            // cleanup here
          }
          simpleFeature
        }
    }

    GeoMesaSpark.apply(dsConf).save(processedRDD, dsConf, featureName)
    println("""Site Loss Analysis Ingestion completed ...""")
  }
}
