package example_java.dataframe;

import example_java.common.JavaData;
import example_java.common.JavaPerson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class JavaDataFrameExample {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        List<JavaPerson> sampleData = JavaData.sampleData();
        DataFrame df = sqlContext.createDataFrame(sampleData, JavaPerson.class);

        // example transformations

        // SQL style
        System.out.println("Under 21");
        df.filter("age < 21")
            .collectAsList()
            .stream().
            forEach(System.out::println);

        // Expression builder style
        System.out.println("Over 21");
        df.filter(df.col("age").gt(21))
            .collectAsList()
            .stream()
            .forEach(System.out::println);



    }
}
