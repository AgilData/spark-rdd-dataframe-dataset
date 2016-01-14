package example.rdd

import org.apache.spark.sql.{Row, SQLContext}
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

    // example tranformation
    rdd.filter(p => p.age == "CO")

  }

}








