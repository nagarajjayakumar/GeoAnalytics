package com.hortonworks.gc.ingest

import java.io.IOException
import java.util
import java.util.Collections

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
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
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
  *
  */
object GeomesaHbaseWrite {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "siteexposure")

  var LATITUDE_COL_IDX = 8
  var LONGITUDE_COL_IDX = 10
  var ID_COL_IDX = 3
  var SHAPE_COL_IDX = 6
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "event"
  val ingestFile =
    "file:///Users/njayakumar/Desktop/GuyCarpenter/workspace/GeoAnalytics/src/main/resources/ingest.txt"

  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long", //0
    "peril_id:java.lang.Long", // 1- some types require full qualification (see DataUtilities docs)
    "account_id:String", //2
    "site_id:String", //3
    "arcgis_id:java.lang.Long", //4
    "nz_grid_id:String", //5
    "shape:String", //6
    "sitenum:String", //7
    "site_lat:java.lang.Double", //8
    "account_num:String", //9
    "site_long:java.lang.Double", //10
    "s_udf_met1:java.lang.Double", //11
    "*geom:Point:srid=4326", // 12 the "*" denotes the default geometry (used for indexing)
    "shapegeom:Point:srid=4326" //13
    //"polygonGeom:Polygon:srid=4326"  // just an example to show the supported Geometry data type.
  )

  val featureType: SimpleFeatureType =
    buildGeomesaSiteExpoEventFeatureType(featureName, attributes)

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore]
    .createSchema(featureType)

  /**
    * This is the method to create the Simple feature.
    * @param value
    * @return
    */
  def createSimpleFeature(value: String): SimpleFeature = {

    val attributes: Array[String] = value.toString.split("\\,", -1)

    featureBuilder.reset
    val lat: Double = attributes(LATITUDE_COL_IDX).toDouble
    val lon: Double = attributes(LONGITUDE_COL_IDX).toDouble

    if (Math.abs(lat) > 90.0 || Math.abs(lon) > 180.0) {
      // log invalid lat/lon
    }

    val geom: Geometry = geometryFactory.createPoint(new Coordinate(lon, lat))

    val simpleFeature: SimpleFeature =
      featureBuilder.buildFeature(attributes(ID_COL_IDX))
    // be sure to tell GeoTools explicitly that you want to use the ID you provided
    simpleFeature.getUserData
      .put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

    var i: Int = 0
    while (i < attributes.length) {
      simpleFeature.setAttribute(i, attributes(i))
      i += 1
    }

    val geometry = WKTUtils.read(attributes(SHAPE_COL_IDX))
    simpleFeature.setAttribute("shapegeom", geometry)
    simpleFeature.setAttribute("geom", geom)
    simpleFeature.setDefaultGeometry(geom)

    simpleFeature
  }

  @throws(classOf[SchemaException])
  def buildGeomesaSiteExpoEventFeatureType(
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
    conf.setMaster("local[3]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator",
             "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")

    val sc = new SparkContext(conf.setAppName("Geomesa Event Ingest"))

    val distDataRDD = sc.textFile(ingestFile)

    val processedRDD: RDD[SimpleFeature] = distDataRDD.mapPartitions {
      valueIterator =>
        if (valueIterator.isEmpty) {
          Collections.emptyIterator
        }

        //  setup code for SimpleFeatureBuilder
        try {
          val featureType: SimpleFeatureType =
            buildGeomesaSiteExpoEventFeatureType(featureName, attributes)
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
    println("ingestion completed ...")
  }
}
