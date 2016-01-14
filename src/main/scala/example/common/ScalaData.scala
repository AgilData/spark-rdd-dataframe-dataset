package example.common

import scala.collection.JavaConversions._
import example_java.common.JavaData

object ScalaData {

  def sampleData() : Seq[ScalaPerson] =
    JavaData.sampleDataAsStrings().map(line => {
      val parts: Array[String] = line.split(",")
      ScalaPerson(parts(0), parts(1), parts(2).toInt, parts(3))
    })

}

case class ScalaPerson(first: String, last: String, age: Int, state: String)
