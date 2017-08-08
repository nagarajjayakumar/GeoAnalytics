package com.hortonworks.gc.query
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object CartesianJoinSiteExpAndWildFire {
  val wildFireDsParams = Map("bigtable.table.name" -> "CatEvents_Fire_2000_US_Wildfire_Footprint")

  val siteExpDsParams =Map("bigtable.table.name" -> "1M_site_exp_portfolio")

  val wildFireDs = DataStoreFinder.getDataStore(wildFireDsParams).asInstanceOf[HBaseDataStore]
  val siteExpDs = DataStoreFinder.getDataStore(siteExpDsParams).asInstanceOf[HBaseDataStore]

  case class site_exp(portfolio_id:Option[Long],account_id:Option[String],site_id:Option[String], s_udf_met1:Option[Double])

  val portfolio_id = "portfolio_id"
  val account_id = "account_id"
  val site_id = "site_id"
  val s_udf_met1 = "s_udf_met1"

  def main(args: Array[String]) {

    val sparkSession = SparkSession
      .builder()
      .appName("geoMesa site exposure Read ")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator",
        "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")
     // .master("local[*]")
      .getOrCreate()

    val sc= sparkSession.sparkContext

    val rddProviderWildFire = GeoMesaSpark(wildFireDsParams)
    val rddProviderSiteExp     = GeoMesaSpark(siteExpDsParams)

    val wildFireRdd: RDD[SimpleFeature] = rddProviderWildFire.rdd(new Configuration(), sc, wildFireDsParams, new Query("wildfireevent"))
    val siteExpRdd: RDD[SimpleFeature] = rddProviderSiteExp.rdd(new Configuration(), sc, siteExpDsParams, new Query("siteexposure_event"))

    val broadcastedCover = sc.broadcast(wildFireRdd.collect)

    // Broadcast sfts to executors
    GeoMesaSparkKryoRegistrator.broadcast(siteExpRdd)

    // Broadcast covering set to executors

    // Key data by cover name
    val keyedData = siteExpRdd.mapPartitions { iter =>
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val ff = CommonFactoryFinder.getFilterFactory2
      val portfolioId = ff.property(portfolio_id)
      val accountId = ff.property(account_id)
      val siteId = ff.property(site_id)
      val sUdfMet1 = ff.property(s_udf_met1)

      iter.map { sf =>
        // Iterate over covers until a match is found
        val it = broadcastedCover.value.iterator
        var container: Option[SimpleFeature] = None

        while (it.hasNext) {
          val cover = it.next()
          // If the cover's polygon contains the feature,
          // or in the case of non-point geoms, if they intersect, set the container
          //val geom: Geometry = geometryFactory.createPoint(new Coordinate(-103.474364, 47.216277))
          if (cover.geometry.contains(sf.geometry))  {
            container = Some(cover)
          }
        }
        // return the found cover as the key
        if (container.isDefined) {
          site_exp(
            Option(portfolioId.evaluate(sf).asInstanceOf[Long]),
            Option(accountId.evaluate(sf).asInstanceOf[String]),
            Option(siteId.evaluate(sf).asInstanceOf[String]),
            Option(sUdfMet1.evaluate(sf).asInstanceOf[Double])
          )
        } else {
          site_exp(None,None,None,None)
        }
      }
    }


    val keyedDataDf = sparkSession.createDataFrame(keyedData)

    val new_df = keyedDataDf.filter("portfolio_id is not null")
    //print(keyedDataDf.collect().length)
    new_df.printSchema()

    new_df.write.format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("header", "true")
      .save("/tmp/results2")
    wildFireDs.dispose()
    siteExpDs.dispose()

  }

}