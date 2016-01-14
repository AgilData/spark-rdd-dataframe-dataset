package example.dataframe

import example.common.ScalaData
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ScalaDataFrameExample {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.createDataFrame(ScalaData.sampleData())

    // example transformations

    // SQL style
    println("Under 21")
    df.filter("age < 21")
      .collect
      .foreach(row => println(row))

    // expression builder style
    println("Over 21")
    df.filter(df.col("age").gt(21))
      .collect
      .foreach(row => println(row))


  }

}








