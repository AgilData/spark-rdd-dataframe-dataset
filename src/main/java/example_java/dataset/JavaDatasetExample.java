package example_java.dataset;

import example_java.common.JavaData;
import example_java.common.JavaPerson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class JavaDatasetExample {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        List<JavaPerson> data = JavaData.sampleData();

        //NOTE: this does not actually work ... throws error "no encoder found for example_java.common.JavaPerson"
        Dataset<JavaPerson> dataset = sqlContext.createDataset(data, Encoders.bean(JavaPerson.class));

        Dataset<JavaPerson> below21 = dataset.filter((FilterFunction<JavaPerson>) person -> (person.getAge() < 21));

        below21.foreach((ForeachFunction<JavaPerson>) person -> System.out.println(person));

    }
}
