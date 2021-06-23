package il.naya.collage.spark.course.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkScalaExample extends App {

  val spark: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate

  import spark.implicits._

  val sampleData: Seq[String] = Seq("one", "two", "three", "four")

  val dataDF: DataFrame = spark.createDataset(sampleData).toDF.withColumnRenamed("value", "my_col")

  dataDF.show

  spark.close

}
