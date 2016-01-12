package example.dataframe

import example.common.Data
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameExample {

    def main(arg: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName("Example")
          .setMaster("local[*]")

        val sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)

        val df = sqlContext.createDataFrame(Data.sampleData())

        // example tranformation
        val df2 = df.filter("state == 'CO'")

        val df3 = df.filter(df.col("age").equalTo("CO"))


        /*p => p.state == "CO")*/

//        // convert to RDD[Row]
//        val rowRdd = rdd.map(person => Row(person.getAge, person))
//
//        // convert to DataFrame (RDD + Schema)
//        val dataFrame = sqlContext.createDataFrame(rowRdd, schema)
//
//        // register as a table
//        dataFrame.registerTempTable("person")
//
//        // selecting the whole object works fine
//        println("Example 1: Select whole object");
//        val results = sqlContext.sql("SELECT person FROM person")
//        val people = results.collect
//        people.map(row => println(row))
//
//        // using functions to extract values works, but does not seem ideal
//        println("Example 2: Select attribute from object using UDF");
//        sqlContext.udf.register("getFirstName", (p: MockPerson) => p.getFirstName)
//        val results2 = sqlContext.sql("SELECT getFirstName(person) FROM person")
//        val people2 = results2.collect
//        people2.map(row => println(row))
//
//        // what I really want to do, is this... but it fails with "Can't extract value from person#1"
//        println("Example 3: Select attribute from object natively");
//        val results3 = sqlContext.sql("SELECT person.firstName FROM person")
//        val people3 = results3.collect
//        people3.map(row => println(row))

      }

    }








