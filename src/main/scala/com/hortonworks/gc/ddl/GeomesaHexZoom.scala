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
  * Geomesa ingest for site loss analysis
  */
object GeomesaHexZoom {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // java -cp /tmp/geoanalytics-1.0-SNAPSHOT-fat.jar   com.hortonworks.gc.ddl.GeomesaHexZoom
  val dsConf = Map("bigtable.table.name" -> "hex_zoom")

  val featureName = "hexzoomevent"

  var attributes = Lists.newArrayList(
    "zoom_id:java.lang.Long" ,
    "zoom_value:java.lang.Long"
  )

  val featureType: SimpleFeatureType =
    buildGeomesaHexZoomEventFeatureType(featureName, attributes)

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore]
    .createSchema(featureType)


  @throws(classOf[SchemaException])
  def buildGeomesaHexZoomEventFeatureType(
      featureName: String,
      attributes: util.ArrayList[String]): SimpleFeatureType = {
    val name = featureName
    val spec = Joiner.on(",").join(attributes)
    val featureType = DataUtilities.createType(name, spec)
    featureType.getUserData.put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "SQLDATE")
    featureType
  }

  def main(args: Array[String]) {

    println("""DDL hexzoomevent completed ...""")
  }
}
