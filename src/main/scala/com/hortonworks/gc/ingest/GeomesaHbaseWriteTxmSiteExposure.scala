package com.hortonworks.gc.ingest

import java.io.IOException
import java.util
import java.util.Collections

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.spark.GeoMesaSpark
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.control.Breaks

/**
  *
  */
object GeomesaHbaseWriteTxmSiteExposure {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "txm_site_exposure_1M")

  var ID_COL_IDX = 0
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "txmsiteexposureevent"

  case class site_exp(portfolio_id:Long,site_id:String, g_country_id:Long, geo_unit_id:Long)

  val portfolio_id = "portfolio_id"
  val site_id = "site_id"
  val g_country_id = "g_country_id"
  val geo_unit_id = "geo_unit_id"


  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long",
    "site_id:String",
    "policy_id:java.lang.Long",
    "g_country_id:java.lang.Long",
    "geo_unit_id:java.lang.Long"
  )

  val featureType: SimpleFeatureType =
    buildGeomesaTxmSiteExposureEventFeatureType(featureName, attributes)

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore].createSchema(featureType)


  /**
    * This is the method to create the Simple feature.
    * @param value
    * @return
    */
  def createSimpleFeature(value : site_exp): SimpleFeature = {

    val simpleFeature: SimpleFeature =
      featureBuilder.buildFeature(attributes(ID_COL_IDX))
    // be sure to tell GeoTools explicitly that you want to use the ID you provided
    simpleFeature.getUserData
      .put(Hints.USE_PROVIDED_FID, java.lang.Boolean.FALSE)

    simpleFeature.setAttribute(0, value.portfolio_id)
    simpleFeature.setAttribute(1, value.site_id)
    simpleFeature.setAttribute(2, value.g_country_id)
    simpleFeature.setAttribute(3, value.geo_unit_id)

    simpleFeature
  }

  @throws(classOf[SchemaException])
  def buildGeomesaTxmSiteExposureEventFeatureType(
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

    val sparkSession = SparkSession
      .builder()
      .appName("Geomesa Transformed Site Exposure Event Ingest")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", 1024*1024*200)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator",
      "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")
        .master("local[3]")
      .getOrCreate()



    val featureTypeName = "siteexposure_event"

    val dataFrame = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "site_exposure_1M"))
      .option("geomesa.feature", featureTypeName)
      .load()


    dataFrame.createOrReplaceTempView(featureTypeName)


    val nzgridevent = "nzgrid_event"

    val dataFramePolicyExposure = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "nzgrid"))
      .option("geomesa.feature", nzgridevent)
      .load()

    dataFramePolicyExposure.createOrReplaceTempView(nzgridevent)


    val sqlQuery2 = "SELECT se.portfolio_id,se.site_id ,nze.g_country_id, nze.geo_unit_id FROM siteexposure_event as  se, nzgrid_event as nze  where se.nz_grid_id=nze.nz_grid_id  "


    val resultDataFrame2 = sparkSession.sql(sqlQuery2)

    val distDataRDD = resultDataFrame2.rdd



    val processedRDD = distDataRDD.mapPartitions {
      valueIterator =>
        if (valueIterator.isEmpty) {
          Collections.emptyIterator
        }

        //  setup code for SimpleFeatureBuilder
        try {
          val featureType: SimpleFeatureType =
            buildGeomesaTxmSiteExposureEventFeatureType(featureName, attributes)
          featureBuilder = new SimpleFeatureBuilder(featureType)
        } catch {
          case e: Exception => {
            throw new IOException("Error setting up feature type", e)
          }
        }

        val ff = CommonFactoryFinder.getFilterFactory2
        val portfolioId = ff.property(portfolio_id)
        val siteId = ff.property(site_id)
        val gcountryid = ff.property(g_country_id)
        val geounitid = ff.property(geo_unit_id)

        valueIterator.map { f =>
          val siteexp = site_exp(
            portfolioId.evaluate(f).asInstanceOf[Long],
            siteId.evaluate(f).asInstanceOf[String],
            gcountryid.evaluate(f).asInstanceOf[Long],
            geounitid.evaluate(f).asInstanceOf[Long]
          )
          val simpleFeature = createSimpleFeature(siteexp)
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
