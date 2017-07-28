package com.hortonworks.gc.query

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.spark.GeoMesaSpark

import scala.collection.JavaConversions._


object GeomesaHbaseReadUsingSparkCore {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x
  // spark-2.0.0/bin/spark-submit --class com.hortonworks.gc.ingest.hbase.GeomesaHbaseReadUsingSparkCore geomesa-utils-15-1.0.jar

  // specify the params for the datastore
  val params = Map("bigtable.table.name" -> "siteexposure")

  // matches the params in the datastore loading code
  val featureTypeName = "event"
  val geom = "geom"

  val portfolio_id = "portfolio_id"
  val account_id = "account_id"
  val site_id = "site_id"
  val s_udf_met1 = "s_udf_met1"

  val bbox = "-94, 31, -96, 29"

  val filter = s"bbox($geom, $bbox) "

  // writes to HDFS - prepend with file:/// for local filesystem
  val outputFile = "file:////tmp/gdeltAttrRDD2016"

  case class site_exp(portfolio_id:Long,account_id:String,site_id:String, s_udf_met1:Double)

  def main(args: Array[String]) {

    // ------------- read from GeoMesa store

    // Get a handle to the data store
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

    // Construct a CQL query to filter by bounding box
    val q = new Query(featureTypeName, ECQL.toFilter(filter))

    // Configure Spark
//    val conf = new SparkConf().setAppName("geoMesa site exposure Read ")
//    conf.setMaster("local[3]")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator",
//             "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")

    val sparkSession = SparkSession
      .builder()
      .appName("geoMesa site exposure Read ")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator",
          "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")
      .master("local[*]")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    // Create an RDD from a query
    val simpleFeaureRDD =
      GeoMesaSpark(params).rdd(new Configuration, sc, params, q)

    // Convert RDD[SimpleFeature] to RDD[Row] for Dataframe creation below
    val gmSiteExposireEventAttrRDD = simpleFeaureRDD.mapPartitions { iter =>
      val ff = CommonFactoryFinder.getFilterFactory2
      val portfolioId = ff.property(portfolio_id)
      val accountId = ff.property(account_id)
      val siteId = ff.property(site_id)
      val sUdfMet1 = ff.property(s_udf_met1)
      iter.map { f =>
        site_exp(
          portfolioId.evaluate(f).asInstanceOf[Long],
          accountId.evaluate(f).asInstanceOf[String],
          siteId.evaluate(f).asInstanceOf[String],
          sUdfMet1.evaluate(f).asInstanceOf[Double]
        )
      }
    }
    val gmSiteExposireEventAttrDf=sparkSession.createDataFrame(gmSiteExposireEventAttrRDD)

    ds.dispose

    println("before")
    print(gmSiteExposireEventAttrRDD.collect().length)
    gmSiteExposireEventAttrRDD.collect().foreach(println)
    gmSiteExposireEventAttrDf.printSchema()
    println("after")
    // use this to save the GeoMesa data
    //gdeltAttrRDD.saveAsObjectFile(outputFile)

  }
}
