package com.hortonworks.gc.udf

object WidthBucket {

  def widthBucket(expr: Double, minValue: Double, maxValue: Double, numBucket: Long): Long = {

    if (numBucket <= 0) {
      throw new RuntimeException(s"The num of bucket must be greater than 0, but got ${numBucket}")
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

}
