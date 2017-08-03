package com.hortonworks.gc

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


object GeoJsonIngestWildFireShapes {

  val dsConf = Map("bigtable.table.name" -> "CatEvents_Fire_2000_US_Wildfire_Footprint")

  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "wildfireevent"
  val ingestFile =
    "/Users/njayakumar/Desktop/GuyCarpenter/workspace/GeoAnalytics/src/main/resources/wildfire.json"

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

    val sparkSession = SparkSession
      .builder()
      .appName("Geomesa Wild Fire GeoJSON IDX")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    val name = "wildfireidx"
//    val index = new GeoJsonGtIndex(ds)
//    index.createIndex(name, Some("$.properties.OBJECTID"), points = false)
//
//    val geoJsonContent = scala.io.Source.fromFile(ingestFile).mkString
//
//    index.add(name, geoJsonContent)
//
//    println(index.query(name, """{ "properties.objectid" : 4 }""").toList)



//    val sparkContext = sparkSession.sparkContext
//
//    val distDataRDD = sparkContext.textFile(ingestFile)
//
//    val processedRDD: RDD[SimpleFeature] = distDataRDD.mapPartitions {
//      valueIterator =>
//        if (valueIterator.isEmpty) {
//          Collections.emptyIterator
//        }
//
//        valueIterator.map { s =>
//          // Processing as before to build the SimpleFeatureType
//          val simpleFeature = createSimpleFeature(s)
//          if (!valueIterator.hasNext) {
//            // cleanup here
//          }
//          simpleFeature
//        }
//    }
//
//
//    processedRDD.take(1)
//    println(processedRDD.take(1))


    val fjson = new FeatureJSON

    val file = new File(ingestFile)
    val fileIn = new FileInputStream(file)


    val featureCollection  = fjson.readFeatureCollection(fileIn)

    println(ingestToDataStore(featureCollection.asInstanceOf[SimpleFeatureCollection],ds,featureName))

    fileIn.close

//    val shapefile =  FileDataStoreFinder.getDataStore(new File(ingestFile))
//
//    val featureCollection  = shapefile.getFeatureSource.getFeatures
//
//    GeneralShapefileIngest.ingestToDataStore(featureCollection,ds,Option(featureName))
//
////    println(featureCollection.size)
////
////    val index = new GeoJsonGtIndex(ds)




    //GeoMesaSpark.apply(dsConf).save(processedRDD, dsConf, featureName)
    //println(processedRDD.size)

    println("ingestion completed ...")
  }


  def ingestToDataStore(features: SimpleFeatureCollection , ds: HBaseDataStore, typeName: String): (Long, Long) = {
    // Add the ability to rename this FT

    ds.createSchema(featureType)

    var count = 0L
    var failed = 0L

    WithClose(ds.getFeatureWriterAppend(featureType.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      SelfClosingIterator(features.features ).foreach { feature =>
        try {
          FeatureUtils.copyToWriter(writer, feature)
          writer.write()
          count += 1
        } catch {
          case NonFatal(e) => {
            println(s"Error writing feature: $feature: $e", e)
            failed += 1
          }
        }
      }
    }

    (count, failed)
  }

}
