package il.naya.collage.spark.course.bdd.four;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class Log4JTester {

    public static void main(String[] args) throws InterruptedException {

        Logger log = Logger.getLogger(Log4JTester.class);

        log.info("Start Spark application");

        SparkSession spark = SparkSession.builder().appName("log application").master("local[*]").getOrCreate();

        log.info("Spark application Started successfully");

        for (int i = 1; i <= 30; i++){
            log.info("Interation number: " + i);
            Thread.sleep(1000);
        }


        log.info("Closing Spark application");

        spark.close();

        log.info("Spark closed successfully");

    }

}
