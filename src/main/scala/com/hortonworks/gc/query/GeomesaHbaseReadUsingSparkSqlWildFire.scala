package com.hortonworks.gc.query

import org.apache.spark.sql.SparkSession

object GeomesaHbaseReadUsingSparkSqlWildFire {

  // works for latest version of Geomesa 1.3.2 + Spark 2.x

  // specify the params for the datastore
  val dsParams = Map("bigtable.table.name" -> "siteexposure")

  // matches the params in the datastore loading code
  val featureTypeName:String = "event"
  val siteLossAnalyzFeatureTypeName = "sitelossanalyzevent"
  val geom = "geom"

  def main(args: Array[String]) {

    // Configure Spark
    val sparkSession = SparkSession
      .builder()
      .appName("testSpark")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "siteexposure"))
      .option("geomesa.feature", featureTypeName)
      .load()

    dataFrame.createOrReplaceTempView(featureTypeName)

    val dataFramewildfireevent = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "CatEvents_Fire_2000_US_Wildfire_Footprint"))
      .option("geomesa.feature", "wildfireevent")
      .load()

    dataFramewildfireevent.createOrReplaceTempView(
      "wildfireevent")


    val sqlQuery =
      "select OBJECTID, ACRES from wildfireevent  where OBJECTID < 100 limit 10"

    val resultDataFrame = sparkSession.sql(sqlQuery)

    resultDataFrame.show

    println("DONE")
  }
}
