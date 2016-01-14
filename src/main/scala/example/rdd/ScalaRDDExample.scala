package example.rdd

import org.apache.spark.{SparkConf, SparkContext}

import example.common.ScalaData

object ScalaRDDExample {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // load initial RDD
    val rdd = sc.parallelize(ScalaData.sampleData())

    // example transformations

    // verbose syntax
    println("Under 21")
    rdd.filter(p => p.age < 21).foreach(println(_))

    // concise syntax
    println("Over 21")
    rdd.filter(_.age > 21).foreach(println(_))

  }

}








