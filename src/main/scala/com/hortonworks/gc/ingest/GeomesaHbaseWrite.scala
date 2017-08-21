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
object GeomesaHbaseWrite {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "site_exposure_1M")

  var LATITUDE_COL_IDX = 9
  var LONGITUDE_COL_IDX = 11
  var ID_COL_IDX = 0


  var SHAPE_COL_IDX = 6
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "siteexposure_event"
  val ingestFile =
      "hdfs:///tmp/geospatial/site_exposure_1M/site_exposure_1M.csv"

  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long", //0
    "peril_id:java.lang.Long", //1
    "account_id:String", //2
    "site_id:String", //3
    "arcgis_id:java.lang.Long", //4
    "nz_grid_id:String", //5
    "shape:String", //6
    "sitenum:String", //7
    "sitename:String", //8
    "site_lat:java.lang.Double", //9
    "account_num:String", // 10
    "site_long:java.lang.Double", //11
    "country_id:java.lang.Long",
    "currency:String",
    "g_mc_level1_id:String",
    "g_mc_level2_id:String",
    "g_mc_level3_id:String",
    "g_mc_level4_id:String",
    "g_mc_level5_id:String",
    "g_mc_level6_id:String",
    "g_level:java.lang.Long",
    "g_level_source_id:java.lang.Long",
    "full_address:String",
    "street_addr:String",
    "municipality:String",
    "postalcode:String",
    "cresta:String",
    "blg_num:java.lang.Long",
    "blg_name:String",
    "lob1:String",
    "lob2:String",
    "occ_short_desc:String",
    "const_short_desc:String",
    "m_air_occind:String",
    "m_air_occ:String",
    "m_air_constind:String",
    "m_air_const:String",
    "m_rms_occind:String",
    "m_rms_occ:String",
    "m_rms_constind:String",
    "m_rms_const:String",
    "num_stories:java.lang.Long",
    "year_built:java.lang.Long",
    "expire_date:String",
    "incept_date:String",
    "cov1val:java.lang.Double",
    "cov2val:java.lang.Double",
    "cov3val:java.lang.Double",
    "cov4val:java.lang.Double",
    "cov5val:java.lang.Double",
    "cov6val:java.lang.Double",
    "risk_count:java.lang.Long",
    "shift_count:java.lang.Long",
    "payroll:java.lang.Double",
    "empl_count:java.lang.Double",
    "calc_num_empl:java.lang.Double",
    "max_empl:java.lang.Double",
    "shift1:java.lang.Double",
    "shift2:java.lang.Double",
    "shift3:java.lang.Double",
    "shift4:java.lang.Double",
    "deduct_type:String",
    "limit_type:String",
    "premium:java.lang.Double",
    "site_limit:java.lang.Double",
    "site_bl_limit:java.lang.Double",
    "site_deduct:java.lang.Double",
    "site_bl_deduct:java.lang.Double",
    "cmb_deduct:java.lang.Double",
    "cmb_limit:java.lang.Double",
    "agg_limit:java.lang.Double",
    "cov1limit:java.lang.Double",
    "cov1deduct:java.lang.Double",
    "cov2limit:java.lang.Double",
    "cov2deduct:java.lang.Double",
    "cov3limit:java.lang.Double",
    "cov3deduct:java.lang.Double",
    "cov4limit:java.lang.Double",
    "cov4deduct:java.lang.Double",
    "cov4days:java.lang.Double",
    "cov5limit:java.lang.Double",
    "cov5deduct:java.lang.Double",
    "cov6limit:java.lang.Double",
    "cov6deduct:java.lang.Double",
    "rms_distance_coast:java.lang.Double",
    "s_udf_met1:java.lang.Double",
    "s_udf_met2:java.lang.Double",
    "s_udf_met3:java.lang.Double",
    "s_udf_met4:java.lang.Double",
    "s_udf_met5:java.lang.Double",
    "s_udf_met6:java.lang.Double",
    "s_udf_met7:java.lang.Double",
    "s_udf_met8:java.lang.Double",
    "s_udf_met9:java.lang.Double",
    "s_udf_met10:java.lang.Double",
    "s_udf_attr1:String",
    "s_udf_attr2:String",
    "s_udf_attr3:String",
    "s_udf_attr4:String",
    "s_udf_attr5:String",
    "s_udf_attr6:String",
    "s_udf_attr7:String",
    "s_udf_attr8:String",
    "s_udf_attr9:String",
    "s_udf_attr10:String",
    "site_comments:String",
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

    val attributes: Array[String] = value.toString.split("\\|", -1)

    featureBuilder.reset
    var lat : Double = 0.0
    try {
      lat = attributes(LATITUDE_COL_IDX).toDouble
    }catch{
      case e: java.lang.NumberFormatException => {println("site_lat " + attributes(LATITUDE_COL_IDX) + " end")}
    }
    var lon: Double = 0.0
    try {
      lon = attributes(LONGITUDE_COL_IDX).toDouble
    }catch{
      case e: java.lang.NumberFormatException => {println("site_long " + attributes(LATITUDE_COL_IDX) + " end")}
    }

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
    val loop = new Breaks
    loop.breakable {
    while (i < (attributes.length - 2)) {
      simpleFeature.setAttribute(i, attributes(i))
      i += 1
    }
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
