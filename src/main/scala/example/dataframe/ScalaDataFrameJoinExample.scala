package example.dataframe

import example.common.ScalaData
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaDataFrameJoinExample {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val left = sqlContext.createDataFrame(sc.parallelize(List(
      Foo(1,"One")
    )))

    val right = sqlContext.createDataFrame(sc.parallelize(List(
      Foo(1,"Uno")
    )))

    left.show()

    val join1 = left.join(right, left.col("id").equalTo(right.col("id")))
    join1.show()

    //NOTE: this would fail, because `name` is ambiguous
    //join1.select(join.col("name")).show()

    // however, we can use a column retrieved from the left dataframe to access data in the join
    join1.select(left.col("name")).show()

    // another approach is to use dataframe aliases
    val join2 = left.as("left").join(right.as("right"), left.col("id").equalTo(right.col("id")))
    join1.show()
    join1.select(join2.col("left.name")).show()
    
  }

}

case class Foo(id: Int, name: String)








