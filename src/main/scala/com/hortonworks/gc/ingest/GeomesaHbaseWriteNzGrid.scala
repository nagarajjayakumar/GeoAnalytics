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
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.control.Breaks

/**
  *
  */
object GeomesaHbaseWriteNzGrid {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "nzgrid")

  var ID_COL_IDX = 15

  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "nzgrid_event"
  val ingestFile =
      "hdfs:///tmp/geospatial/nzgrid/mc_guid_geom_nzgrid_view.csv"

  var attributes = Lists.newArrayList(
    "OBJECTID:java.lang.Long", //0
    "g_country_id:java.lang.Long", //1
    "g_mc_level1_id:java.lang.Long", //2
    "g_mc_level2_id:java.lang.Long", //3
    "g_mc_level3_id:java.lang.Long", //4
    "g_mc_level4_id:java.lang.Long", //5
    "g_mc_level5_id:java.lang.Long", //6
    "g_mc_level6_id:java.lang.Long", //7
    "g_mc_level1_wgt:java.lang.Double", //8
    "g_mc_level2_wgt:java.lang.Double", //9
    "g_mc_level3_wgt:java.lang.Double", //10
    "g_mc_level4_wgt:java.lang.Double", //11
    "g_mc_level5_wgt:java.lang.Double", //12
    "g_mc_level6_wgt:java.lang.Double", //13
    "geo_unit_id:java.lang.Long", //14
    "nz_grid_id:java.lang.Long",  //15
    "maxx:java.lang.Double",  //16
    "maxy:java.lang.Double",  //17
    "minx:java.lang.Double",  //18
    "miny:java.lang.Double",  //19
    "*SHAPE:Polygon:srid=4326" //20
      //"polygonGeom:Polygon:srid=4326"  // just an example to show the supported Geometry data type.
  )

  val featureType: SimpleFeatureType =
    buildGeomesaNzGridEventFeatureType(featureName, attributes)

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
  def buildGeomesaNzGridEventFeatureType(
      featureName: String,
      attributes: util.ArrayList[String]): SimpleFeatureType = {
    val name = featureName
    val spec = Joiner.on(",").join(attributes)
    val featureType = DataUtilities.createType(name, spec)
    //featureType.getUserData.put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "SQLDATE")
    //featureType.getDescriptor("site_id").getUserData.put("index", "true")
    featureType
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()
    // conf.setMaster("local[3]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator",
             "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")

    val sc = new SparkContext(conf.setAppName("Geomesa Event Ingest"))

    val distDataRDD = sc.textFile(ingestFile)

    val processedRDD: RDD[SimpleFeature] = distDataRDD.mapPartitionsWithIndex {
      (idx, valueIterator) =>

        if (idx == 0) valueIterator.drop(1)

        if (valueIterator.isEmpty) {
          Collections.emptyIterator
        }

        //  setup code for SimpleFeatureBuilder
        try {
          val featureType: SimpleFeatureType =
            buildGeomesaNzGridEventFeatureType(featureName, attributes)
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
