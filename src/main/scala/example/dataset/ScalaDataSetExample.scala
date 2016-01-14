package example.dataset

import example.common.ScalaData
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.{SparkConf, SparkContext}

object ScalaDataSetExample {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //TODO: not working yet
    /*
    val dataset = sqlContext.createDataset(ScalaData.sampleData())

    // example transformation

    dataset.filter(p => p.age < 21)
      .collect
      .foreach(println(_))
    */

  }

}








