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
  * Geomesa ingest for site loss analysis
  */
object GeomesaHbaseWriteSiteLossAnalysis {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseWrite geomesa-utils-15-1.0.jar

  val dsConf = Map("bigtable.table.name" -> "site_loss_analysis_1M")

  var ID_COL_IDX = 0
  var featureBuilder: SimpleFeatureBuilder = null
  var geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val featureName = "sitelossanalyzevent"
  //val ingestFile =
  //  "hdfs://usdf23v0377.mrshmc.com:8020/tmp/geospatial/site_loss_analysis_1M/site_loss_analysis_1M.csv"

  val ingestFile = "hdfs://usdf23v0377.mrshmc.com:8020/tmp/ingest_site_loss_analysis_csv.txt"
  var attributes = Lists.newArrayList(
    "portfolio_id:java.lang.Long", //0
    "analysis_id:java.lang.Long", //1
    "site_id:String", //2
    "account_id:String", //3
    "vendor_id:java.lang.Long", //4
    "model_id:java.lang.Long",  //5
    "peril_id:java.lang.Long", // 6 - some types require full qualification (see DataUtilities docs)
    "gross_loss:java.lang.Double", //7
    "damage_loss:java.lang.Double", //8
    "net_loss:java.lang.Double", //9
    "occ_id:java.lang.Long", //10
    "occ_ind:String", //11
    "const_id:java.lang.Long", //12
    "const_ind:String" //13
  )

  val featureType: SimpleFeatureType =
    buildGeomesaSiteLossAnalyzEventFeatureType(featureName, attributes)

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
  def buildGeomesaSiteLossAnalyzEventFeatureType(
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
    //conf.setMaster("local[3]")
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
            buildGeomesaSiteLossAnalyzEventFeatureType(featureName, attributes)
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
