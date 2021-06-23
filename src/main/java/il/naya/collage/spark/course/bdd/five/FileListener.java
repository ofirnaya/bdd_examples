package il.naya.collage.spark.course.bdd.five;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class FileListener {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        String bootstrapServer = "kafka:9092";

        SparkSession spark = SparkSession.builder().appName("File Listener").master("local[*]").getOrCreate();

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
                .option("checkpointLocation", "hdfs://hdfs:8020/checkpoints/file_listener")
                .start();

        boolean isFinish = false;

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://hdfs:8020/");
        FileSystem fs = FileSystem.get(conf);

        while (!isFinish){
            try{
                fs.listStatus(new Path("hdfs://hdfs:8020/tmp/stop_app.file"));

                isFinish = true;
            } catch (Exception e){

            }

            Thread.sleep(1000);
        }

        fs.delete(new Path("hdfs://hdfs:8020/tmp/stop_app.file"), true);

        query.stop();
        spark.close();


    }

}
