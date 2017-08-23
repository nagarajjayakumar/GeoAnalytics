package com.hortonworks.gc.ingest

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
object GeomesaHbaseWriteTxmHexZoom {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWriteTxmHexZoom geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "txm_site_hex_bin")

  var ID_COL_IDX = 0
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "txmsitehexbinevent"

  case class site_exp(portfolio_id:Long,account_id:String, site_id:String, lod:Long, arow:Long, acol:Long)

  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long",
    "account_id:String",
    "site_id:String",
    "lod:java.lang.Long",
    "arow:java.lang.Long",
    "acol:java.lang.Long"
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

    println("""GeomesaHbaseWriteTxmHexZoom Ingestion Started  ...""")

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




    // Site Exposure
    val featureTypeName = "siteexposure_event"

    val dataFrame = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "site_exposure_1M"))
      .option("geomesa.feature", featureTypeName)
      .load()

    dataFrame.createOrReplaceTempView(featureTypeName)


    val hexzoomevent = "hexzoomevent"

    val dataFrameHexZoom = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "hex_zoom"))
      .option("geomesa.feature", hexzoomevent)
      .load()

    dataFrameHexZoom.createOrReplaceTempView(hexzoomevent)


    //val sqlQuery = "SELECT count(*)  FROM   siteexposure_event, hexzoomevent  WHERE  portfolio_id =51325 AND    st_isempty(geom) = false "

    // Temporary View to create 20 ZOOM level between the site exposure and the Hex zoom
    val sqlQuery = "SELECT portfolio_id,account_id,site_id,geom,st_x(geom) AS x,st_y(geom) AS y,zoom_id AS lod  FROM   siteexposure_event, hexzoomevent  WHERE  portfolio_id =51325 AND    st_isempty(geom) = false "
    val resultDataFrame = sparkSession.sql(sqlQuery)
    resultDataFrame.createOrReplaceTempView("siteexp_51325_tmp")

    // Temporary view for the   siteexp_51325_tmp_l1
    val sqlQuery_1 = "SELECT se.portfolio_id,  se.account_id,  se.site_id,  se.lod,  se.geom,  se.x,   se.y,   aphex.zoom_id,  aphex.zoom_value     AS hex_radius,  20037507.0671618  AS origin_x,  19971868.8804086  AS origin_y   FROM  siteexp_51325_tmp AS se  JOIN hexzoomevent AS aphex ON se.lod=aphex.zoom_id limit 10 "

    val resultDataFrame_1 = sparkSession.sql(sqlQuery_1)
    resultDataFrame_1.createOrReplaceTempView("siteexp_51325_tmp_l1")

    // Temporary view for the   siteexp_51325_tmp_l2
    val sqlQuery_2 = "SELECT l1.portfolio_id,   l1.account_id,   l1.site_id,   l1.lod,   l1.geom,   l1.x,   l1.y,   l1.zoom_id,   l1.hex_radius,   l1.origin_x,   l1.origin_y,   l1.x - l1.origin_x AS dx,  l1.y - l1.origin_y AS dy,  l1.hex_radius*cos(30.0*Pi()/180.0) AS h,  l1.hex_radius / 2.0 AS v from siteexp_51325_tmp_l1 as l1 "

    val resultDataFrame_2 = sparkSession.sql(sqlQuery_2)
    resultDataFrame_2.createOrReplaceTempView("siteexp_51325_tmp_l2")


    // Temporary view for the   siteexp_51325_tmp_l3
    val sqlQuery_3 = "SELECT l2.portfolio_id,   l2.account_id,   l2.site_id,   l2.lod,   l2.geom,   l2.x,   l2.y,   l2.zoom_id,   l2.hex_radius,   l2.origin_x,   l2.origin_y,   l2.dx,   l2.dy,   l2.h,   l2.v,   floor(dy/(3.0*l2.v)) AS quad_row,  floor(dx/(2.0*l2.h)) AS quad_col,  floor(dy/(3.0*l2.v))%2 AS mod2_row from siteexp_51325_tmp_l2 as l2 "

    val resultDataFrame_3 = sparkSession.sql(sqlQuery_3)
    resultDataFrame_3.createOrReplaceTempView("siteexp_51325_tmp_l3")

    // Temporary view for the   siteexp_51325_tmp_l4
    val sqlQuery_4 = "SELECT l3.portfolio_id,   l3.account_id,   l3.site_id,   l3.lod,   l3.geom,   l3.x,   l3.y,   l3.zoom_id,   l3.hex_radius,   l3.dx,   l3.dy,   l3.h,   l3.v,   l3.quad_row,   l3.quad_col,   l3.mod2_row,   CASE     WHEN l3.mod2_row <> 0  THEN l3.quad_col*(2.0 * l3.h) + l3.h + l3.origin_x ELSE l3.quad_col * (2.0 * l3.h) + 0.0 + l3.origin_x END AS center_x, l3.quad_row*(3.0*l3.v) +l3.origin_y AS center_y from siteexp_51325_tmp_l3 as l3 "

    val resultDataFrame_4 = sparkSession.sql(sqlQuery_4)
    resultDataFrame_4.createOrReplaceTempView("siteexp_51325_tmp_l4")

    // Temporary view for the   siteexp_51325_tmp_l5
    val sqlQuery_5 = "SELECT l4.portfolio_id,   l4.account_id,   l4.site_id,   l4.lod,   l4.geom,   l4.x,   l4.y,   l4.zoom_id,   l4.hex_radius,   l4.dx,   l4.dy,   l4.h,   l4.v,   l4.quad_row,   l4.quad_col,   l4.mod2_row,   l4.center_x,   l4.center_y,   l4.x - l4.center_x AS q2x,   l4.y - l4.center_y AS q2y,   abs(l4.x - l4.center_x) AS abs_q2x,   abs(l4.y - l4.center_y) AS abs_q2y  from siteexp_51325_tmp_l4 as l4 "

    val resultDataFrame_5 = sparkSession.sql(sqlQuery_5)
    resultDataFrame_5.createOrReplaceTempView("siteexp_51325_tmp_l5")


    // Temporary view for the   siteexp_51325_tmp_l6
    val sqlQuery_6 = "SELECT l5.portfolio_id,   l5.account_id,   l5.site_id,   l5.lod,   l5.geom,   l5.x,   l5.y,   l5.zoom_id,   l5.hex_radius,   l5.dx,   l5.dy,   l5.h,   l5.v,   l5.quad_row,   l5.quad_col,   l5.mod2_row,   l5.center_x,   l5.center_y,   l5.q2x,   l5.q2y,   l5.abs_q2x,   l5.abs_q2y,   atan2 (l5.q2x,l5.q2y) AS arctangent  from siteexp_51325_tmp_l5 as l5 "

    val resultDataFrame_6 = sparkSession.sql(sqlQuery_6)
    resultDataFrame_6.createOrReplaceTempView("siteexp_51325_tmp_l6")

    // Temporary view for the   siteexp_51325_tmp_l7
    val sqlQuery_7 = "SELECT l6.portfolio_id,   l6.account_id,   l6.site_id,   l6.lod,   l6.quad_row,   l6.quad_col,   CAST(l6.mod2_row AS DOUBLE) AS mod2_row,   CASE     WHEN     l6.abs_q2x > l6.hex_radius OR l6.abs_q2y > l6.hex_radius     THEN false     ELSE ((l6.abs_q2x/l6.h)+(l6.abs_q2y/l6.v)) <= 2.0   END AS is_inside,   CASE    WHEN arctangent < 0    THEN (arctangent + (2.0 * Pi())) * 180.0 / Pi()    ELSE arctangent * 180.0 / Pi()   END   AS azimuth  from siteexp_51325_tmp_l6 as l6 "

    val resultDataFrame_7 = sparkSession.sql(sqlQuery_7)
    resultDataFrame_7.createOrReplaceTempView("siteexp_51325_tmp_l7")

    // Temporary view for the   siteexp_51325_tmp_l8
    val sqlQuery_8 = "SELECT portfolio_id,   account_id,   site_id,   lod,   CASE     WHEN is_inside THEN quad_row     WHEN NOT is_inside    AND azimuth > 300.0 THEN quad_row + 1     WHEN NOT is_inside    AND azimuth > 240.0 THEN quad_row     WHEN NOT is_inside    AND azimuth > 180.0 THEN quad_row - 1     WHEN NOT is_inside    AND azimuth > 120.0 THEN quad_row - 1     WHEN NOT is_inside    AND azimuth > 60.0 THEN quad_row     ELSE quad_row + 1   END AS arow,   CASE     WHEN is_inside THEN quad_col     WHEN NOT is_inside    AND mod2_row <> 0.0    AND azimuth > 300.0 THEN quad_col     WHEN NOT is_inside    AND mod2_row = 0.0    AND azimuth > 300.0 THEN quad_col - 1     WHEN NOT is_inside    AND azimuth > 240.0 THEN quad_col - 1     WHEN NOT is_inside    AND mod2_row <> 0.0    AND azimuth > 180.0 THEN quad_col - 1     WHEN NOT is_inside    AND mod2_row = 0.0    AND azimuth > 180.0 THEN quad_col     WHEN NOT is_inside    AND mod2_row <> 0.0    AND azimuth > 120.0 THEN quad_col + 1     WHEN NOT is_inside    AND mod2_row = 0.0    AND azimuth > 120.0 THEN quad_col     WHEN NOT is_inside    AND azimuth > 60.0 THEN quad_col + 1     WHEN NOT is_inside    AND mod2_row <> 0.0    AND azimuth > 0.0 THEN quad_col + 1     ELSE quad_col   END AS acol  from siteexp_51325_tmp_l7 "

    val resultDataFrame_8 = sparkSession.sql(sqlQuery_8)

    resultDataFrame_8.createOrReplaceTempView("siteexp_51325_tmp_l8")

    resultDataFrame_7
      .write
      .format("geomesa")
      .options(dsConf)
      .option("geomesa.feature",featureName)
      .save()

    println("""GeomesaHbaseWriteTxmHexZoom Ingestion completed ...""")
  }
}
