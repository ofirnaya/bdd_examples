package il.naya.collage.spark.course.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkJavaExample {

    public static void main(String[] args){

        SparkSession spark = SparkSession.builder().master("local[*]").appName("example_app").getOrCreate();

        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create("one"));
        list.add(RowFactory.create("two"));
        list.add(RowFactory.create("three"));
        list.add(RowFactory.create("four"));

        List<StructField> listOfStructField=
                new ArrayList<>();
        listOfStructField.add
                (DataTypes.createStructField("my_col", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(listOfStructField);

        Dataset<Row> dataDF = spark.createDataFrame(list,structType);

        dataDF.show();

        spark.close();

    }

}
