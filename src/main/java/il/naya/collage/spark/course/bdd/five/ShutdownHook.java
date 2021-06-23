package il.naya.collage.spark.course.bdd.five;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class ShutdownHook {

    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        String bootstrapServer = "kafka:9092";

        SparkSession spark = SparkSession.builder().appName("Shutdown Hook").master("local[*]").getOrCreate();

        Dataset<Row> dataDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("subscribe", "my-topic")
                .load()
                .select(col("value").cast(StringType).alias("value"));

        StreamingQuery query = dataDF
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option("topic", "junk-topic")
                .option("checkpointLocation", "hdfs://hdfs:8020/checkpoints/shutdown_hook")
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                query.stop();
                while(query.isActive()){}
                spark.close();
            }
        });

        query.awaitTermination();

        spark.close();
    }

}
