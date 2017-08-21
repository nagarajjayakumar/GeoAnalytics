package com.hortonworks.gc.ingest

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
object GeomesaHbaseWritePortfolioAccount {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "portfolio_account_1M")

  var ID_COL_IDX = 0
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "portfolioaccountevent"
  val ingestFile =
    "hdfs:///tmp/geospatial/portfolio_account_1M/portfolio_account_1M.csv"

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

    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator",
             "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")

    // Create Schema

    val sc = new SparkContext(conf.setAppName("Geomesa Portfolio Account Event Ingest"))

    val distDataRDD = sc.textFile(ingestFile)

    val processedRDD: RDD[SimpleFeature] = distDataRDD.mapPartitions {
      valueIterator =>
        if (valueIterator.isEmpty) {
          Collections.emptyIterator
        }

        //  setup code for SimpleFeatureBuilder
        try {
          val featureType: SimpleFeatureType =
            buildGeomesaPortfolioAccountEventFeatureType(featureName, attributes)
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
    println("""Site Loss Analysis Ingestion completed ...""")
  }
}
