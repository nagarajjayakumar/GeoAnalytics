package com.hortonworks.gc.ddl

import java.util

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.sql.SparkSession
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
  *
  */
object GeomesaTxmSiteExposure {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // java -cp /tmp/geoanalytics-1.0-SNAPSHOT-fat.jar   com.hortonworks.gc.ddl.GeomesaTxmSiteExposure

  val dsConf = Map("bigtable.table.name" -> "txm_site_exposure_1M")
  val featureName = "txmsiteexposureevent"

  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long",
    "site_id:String",
    "g_country_id:java.lang.Long",
    "geo_unit_id:java.lang.Long"
  )

  val featureType: SimpleFeatureType =
    buildGeomesaTxmSiteExposureEventFeatureType(featureName, attributes)

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore].createSchema(featureType)

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

    println("""DDL txmsiteexposureevent completed ...""")
  }
}
