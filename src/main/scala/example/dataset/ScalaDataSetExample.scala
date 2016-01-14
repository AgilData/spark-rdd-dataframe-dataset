package example.dataset

import example.common.ScalaData
import org.apache.spark.sql.SQLContext

import org.apache.spark.{SparkConf, SparkContext}

object ScalaDataSetExample {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val dataset = sqlContext.createDataset(ScalaData.sampleData())

    // example transformation

    dataset.filter(p => p.age < 21)
      .collect
      .foreach(println(_))

  }

}








