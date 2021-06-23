package il.naya.collage.spark.course.bdd.three;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Function2;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class StructuredStreamingComplete {

    public static void main(String[] args){
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        String bootstrapServer = "kafka:9092";

        SparkSession spark = SparkSession.builder().appName("agg application").master("local[*]").getOrCreate();

        Dataset<Row> inputDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("subscribe", "spark-topic")
                .load()
                .select(col("value").cast(StringType).alias("value"));

        Dataset<Row> extractedFromJsonDF = inputDF
                .withColumn("is_even", json_tuple(col("value"), "is_even"));

        Dataset<Row> evenOddCounterColsDF = extractedFromJsonDF
                .withColumn("is_even_counter",
                        when(col("is_even").equalTo(lit(true)),
                                lit(1))
                                .otherwise(lit(0)))
                .withColumn("is_odd_counter",
                        when(col("is_even").equalTo(lit(false)),
                                lit(1))
                                .otherwise(lit(0)));


        Dataset<Row> outputDF = evenOddCounterColsDF.agg(
                sum(col("is_even_counter")).alias("even_counter"),
                sum(col("is_odd_counter")).alias("odd_counter"));

        Dataset<Row> outputAsJsonDF = outputDF.toJSON().toDF();

        StreamingQuery query = outputAsJsonDF
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "structred-streaming-agg")
                .option("checkpointLocation", "hdfs://hdfs:8020/checkpoints/ss_agg")
                .outputMode(OutputMode.Complete())
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }

    }
}
