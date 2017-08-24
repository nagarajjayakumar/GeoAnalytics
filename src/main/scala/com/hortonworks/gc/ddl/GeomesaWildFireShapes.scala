package com.hortonworks.gc.ddl

import java.io.{File, FileInputStream}
import java.util

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.sql.SparkSession
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.SchemaException
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.control.NonFatal


object GeomesaWildFireShapes {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // java -cp /tmp/geoanalytics-1.0-SNAPSHOT-fat.jar   com.hortonworks.gc.ddl.GeomesaWildFireShapes

  val dsConf = Map("bigtable.table.name" -> "CatEvents_Fire_2000_US_Wildfire_Footprint")
  val featureName = "wildfireevent"

  var attributes = Lists.newArrayList(
    "OBJECTID:java.lang.Long",
    "ArcGIS_DBO_CatEvents_Fire_2000:Double",
    "PERIMETER:Double",
    "FIRE_NAME:String",
    "COMPLEX:String",
    "ACRES:Double",
    "MILES:Double",
    "YEAR_:String",
    "AGENCY:String",
    "UNITID:String",
    "FIRENUM:String",
    "TIME_:String",
    "METHOD:String",
    "SOURCE:String",
    "TRAVEL:String",
    "MAPSCALE:String",
    "PROJECTION:String",
    "UNITS:String",
    "DATUM:String",
    "COMMENTS:String",
    "Shape_STArea__:Double",
    "Shape_STLength__:Double",
    "SHAPE.STArea:Double",
    "SHAPE.STLength:Double",
    "*geometry:Polygon:srid=4326"

  )

  val featureType: SimpleFeatureType =
    buildGeomesaWidFireEventFeatureType(featureName, attributes)

  @throws(classOf[SchemaException])
  def buildGeomesaWidFireEventFeatureType(
                                                  featureName: String,
                                                  attributes: util.ArrayList[String]): SimpleFeatureType = {
    val name = featureName
    val spec = Joiner.on(",").join(attributes)
    val featureType = DataUtilities.createType(name, spec)
    featureType.getUserData.put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "SQLDATE")
    featureType
  }

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore]

  def main(args: Array[String]) {

    println("DDL GeomesaWildFireShapes completed ...")
  }



}
