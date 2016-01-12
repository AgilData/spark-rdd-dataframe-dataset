    package com.theotherandygrove

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{Row, SQLContext}
    import org.apache.spark.{SparkConf, SparkContext}

    object Example {

      def main(arg: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName("Example")
          .setMaster("local[*]")

        val sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)

        val schema = StructType(List(
          StructField("person_id", DataTypes.IntegerType, true),
          StructField("person", new MockPersonUDT, true)))

        // load initial RDD
        val rdd = sc.parallelize(List(
          MockPersonImpl("James,Jones,21,CO"),
          MockPersonImpl("Basil,Brush,44,AK")
        ))

        // convert to RDD[Row]
        val rowRdd = rdd.map(person => Row(person.getAge, person))

        // convert to DataFrame (RDD + Schema)
        val dataFrame = sqlContext.createDataFrame(rowRdd, schema)

        // register as a table
        dataFrame.registerTempTable("person")

        // selecting the whole object works fine
        println("Example 1: Select whole object");
        val results = sqlContext.sql("SELECT person FROM person")
        val people = results.collect
        people.map(row => println(row))

        // using functions to extract values works, but does not seem ideal
        println("Example 2: Select attribute from object using UDF");
        sqlContext.udf.register("getFirstName", (p: MockPerson) => p.getFirstName)
        val results2 = sqlContext.sql("SELECT getFirstName(person) FROM person")
        val people2 = results2.collect
        people2.map(row => println(row))
        
        // what I really want to do, is this... but it fails with "Can't extract value from person#1"
        println("Example 3: Select attribute from object natively");
        val results3 = sqlContext.sql("SELECT person.firstName FROM person")
        val people3 = results3.collect
        people3.map(row => println(row))

      }

    }

    trait MockPerson {
      def getFirstName: String
      def getLastName: String
      def getAge: Integer
      def getState: String
    }

    class MockPersonUDT extends UserDefinedType[MockPerson] {

      override def sqlType: DataType = StructType(List(
        StructField("firstName", StringType, nullable=false),
        StructField("lastName", StringType, nullable=false),
        StructField("age", IntegerType, nullable=false),
        StructField("state", StringType, nullable=false)
      ))

      override def userClass: Class[MockPerson] = classOf[MockPerson]

      override def serialize(obj: Any): Any = obj.asInstanceOf[MockPersonImpl].data

      override def deserialize(datum: Any): MockPerson = MockPersonImpl(datum.asInstanceOf[String])
    }

    /** This is a contrived use case, but imagine that the person is backed by off-heap memory instead */
    @SQLUserDefinedType(udt = classOf[MockPersonUDT])
    @SerialVersionUID(123L)
    case class MockPersonImpl(data: String) extends MockPerson with Serializable {
      def getFirstName = data.split(",")(0)
      def getLastName = data.split(",")(1)
      def getAge = Integer.parseInt(data.split(",")(2))
      def getState = data.split(",")(3)
    }


