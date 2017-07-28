package com.hortonworks.gc

import org.apache.spark.{SparkConf, SparkContext}

/** * Created by NJAYAKUMAR on 22/07/2017. */

object TestSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    println(sc)
    println(sc.version)

  }
}