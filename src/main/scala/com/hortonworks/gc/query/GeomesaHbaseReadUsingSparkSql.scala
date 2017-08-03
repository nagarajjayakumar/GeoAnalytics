package com.hortonworks.gc.query

import org.apache.spark.sql.SparkSession

object GeomesaHbaseReadUsingSparkSql {

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

    val dataFrameSiteLossAnalyz = sparkSession.read
      .format("geomesa")
      .options(Map("bigtable.table.name" -> "sitelossanalysis"))
      .option("geomesa.feature", siteLossAnalyzFeatureTypeName)
      .load()

    dataFrameSiteLossAnalyz.createOrReplaceTempView(
      siteLossAnalyzFeatureTypeName)

    val sqlContext = sparkSession.sqlContext

    sqlContext.udf.register(
      "WIDTH_BUCKET",
      (expr: Double, minValue: Double, maxValue: Double, numBucket: Long) => {

        if (numBucket <= 0) {
          throw new RuntimeException(
            s"The num of bucket must be greater than 0, but got ${numBucket}")
        }

        val lower: Double = Math.min(minValue, maxValue)
        val upper: Double = Math.max(minValue, maxValue)

        val result: Long = if (expr < lower) {
          0
        } else if (expr >= upper) {
          numBucket + 1L
        } else {
          (numBucket.toDouble * (expr - lower) / (upper - lower) + 1).toLong
        }

        if (minValue > maxValue) (numBucket - result) + 1 else result
      }
    )

    // Query against the "event" schema

//    val sqlQuery =
//      "select event.portfolio_id, sum(event.s_udf_met1), avg(event.s_udf_met1), sum (sitelossanalyzevent.gross_loss) from event  LEFT OUTER JOIN sitelossanalyzevent ON event.site_id = sitelossanalyzevent.site_id  where st_contains(st_makeBBOX(-94.0, 31.0, -96.0, 29.0), geom) group by event.portfolio_id"

    val sqlQuery =
      "select event.portfolio_id, sum(event.s_udf_met1), avg(event.s_udf_met1), sum (sitelossanalyzevent.gross_loss) from event  LEFT OUTER JOIN sitelossanalyzevent ON event.site_id = sitelossanalyzevent.site_id    group by event.portfolio_id"

    val resultDataFrame = sparkSession.sql(sqlQuery)

    resultDataFrame.show

    println("DONE")
  }
}
