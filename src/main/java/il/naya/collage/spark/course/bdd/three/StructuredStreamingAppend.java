package il.naya.collage.spark.course.bdd.three;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class StructuredStreamingAppend {

    public static void main(String[] args){
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        String bootstrapServer = "kafka:9092";

        SparkSession spark = SparkSession.builder().appName("append application").master("local[*]").getOrCreate();

        UDF1<String, String> upLowUDF = new UDF1<String, String>() {
            @Override
            public String call(String input) throws Exception {
                return upLowStr(input);
            }
        };

        spark.udf().register("up_low_str", upLowUDF, StringType);

        Dataset<Row> inputDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("subscribe", "spark-topic")
                .load();

        inputDF.printSchema();

        inputDF = inputDF.select(col("value").cast(StringType).alias("value"));

        inputDF.printSchema();

        StructType jsonSchema = new StructType()
                .add(new StructField("message_id", IntegerType, true, null))
                .add(new StructField("is_even", BooleanType, true, null))
                .add(new StructField("next_message_id", IntegerType, true, null));

        Dataset<Row> parsedJsonDF = inputDF
                .withColumn("parsed_json", from_json(col("value"), jsonSchema))
                .select("parsed_json.*");

        Dataset<Row> outputDF = parsedJsonDF
                .withColumn("rand_value", rand().multiply(100000).mod(100).cast(IntegerType))
                .withColumn("mod_ten_fact",
                        factorial(col("message_id").mod(lit(10)).cast(IntegerType)))
                .withColumn("ind_even_up_low",
                        callUDF("up_low_str", col("is_even").cast(StringType)));

        Dataset<Row> outputAsJsonDF = outputDF.toJSON().toDF();

        StreamingQuery query = outputAsJsonDF
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "structured-streaming-append")
                .option("checkpointLocation", "hdfs://hdfs:8020/checkpoints/ss_append")
                .outputMode(OutputMode.Append())
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }

    }

    public static String upLowStr(String in){
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < in.length(); i++){
            String currentChar = in.substring(i, i+1);
            if (i % 2 == 0){
                sb.append(currentChar.toUpperCase());
            }else{
                sb.append(currentChar.toLowerCase());
            }
        }

        return sb.toString();
    }

}
