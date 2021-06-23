package il.naya.collage.spark.course.example

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes.StringType

object StructuredStreaming extends App {

  System.setProperty("HADOOP_USER_NAME", "hdfs")

  val bootstrapServer = "kafka:9092"

  val spark = SparkSession.builder.appName("agg application").master("local[*]").getOrCreate

  val inputDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("subscribe", "spark-topic")
    .load
    .select(col("value").cast(StringType).alias("value"))

  inputDF.writeStream.format("kafka")
  inputDF.writeStream.format("parquet")

  inputDF.writeStream
    .foreachBatch((df, batchId) => {
      df.cache
      df.write.parquet("hdfs:....")
      df.write.format("kafka").save
      df.unpersist
    })
    .start()

  spark.close

}
