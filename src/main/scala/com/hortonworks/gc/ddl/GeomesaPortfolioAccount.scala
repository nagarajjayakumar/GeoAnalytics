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
  * Geomesa ingest for Portfolio Account
  */
object GeomesaPortfolioAccount {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // java -cp /tmp/geoanalytics-1.0-SNAPSHOT-fat.jar   com.hortonworks.gc.ddl.GeomesaPortfolioAccount

  val dsConf = Map("bigtable.table.name" -> "portfolio_account_1M")
  val featureName = "portfolioaccountevent"

  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long",
    "account_id:java.lang.Long",
    "account_name:String",
    "account_num:java.lang.Long",
    "company:String",
    "division:String",
    "producer:String",
    "branch:String",
    "peril_id:java.lang.Long",
    "a_udf_met1:java.lang.Double",
    "a_udf_met2:java.lang.Double",
    "a_udf_met3:java.lang.Double",
    "a_udf_met4:java.lang.Double",
    "a_udf_met5:java.lang.Double",
    "a_udf_attr1:java.lang.Double",
    "a_udf_attr2:java.lang.Double",
    "a_udf_attr3:java.lang.Double",
    "a_udf_attr4:java.lang.Double",
    "a_udf_attr5:java.lang.Double",
    "a_udf_attr6:java.lang.Double",
    "a_udf_attr7:java.lang.Double",
    "a_udf_attr8:java.lang.Double",
    "a_udf_attr9:java.lang.Double",
    "a_udf_attr10:java.lang.Double"
  )

  val featureType: SimpleFeatureType =
    buildGeomesaPortfolioAccountEventFeatureType(featureName, attributes)

  val ds = DataStoreFinder
    .getDataStore(dsConf)
    .asInstanceOf[HBaseDataStore].createSchema(featureType)


  @throws(classOf[SchemaException])
  def buildGeomesaPortfolioAccountEventFeatureType(
      featureName: String,
      attributes: util.ArrayList[String]): SimpleFeatureType = {
    val name = featureName
    val spec = Joiner.on(",").join(attributes)
    val featureType = DataUtilities.createType(name, spec)
    featureType.getUserData.put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "SQLDATE")
    featureType
  }

  def main(args: Array[String]) {
    println("""DDL Portfolio Account completed ...""")
  }
}
