package example.common

object Data {

  def sampleData() : Seq[Person] = List(
    //TODO load data from file instead, or generate some data here
    Person("", "", 1, "")
  )

}

case class Person(first: String, last: String, age: Int, state: String)
