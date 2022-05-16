package spark.learn;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.util.StringUtils;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkFileReader {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setAppName("spark_reader").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(config);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //load partitions of this file
        JavaRDD<String> fileRdd = sc.textFile("src/main/resources/input.txt");

        //Boring Words
        String filters = "";
        try {
            InputStream is = SparkFileReader.class.getClassLoader().getResourceAsStream("boringwords.txt");
            filters = IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        //partitions by 64mb? based on hadoop
        System.out.println("There are " + fileRdd.getNumPartitions() + " partitions");

        //split
        //use another variable for final filters**
        String finalFilters = filters;
        fileRdd.flatMap(str -> Arrays.asList(str.split(" ")).iterator())
                .filter(str -> !finalFilters.contains(str.toLowerCase()) && str.length() > 1)
                .filter(str -> str.matches("[a-zA-Z]+"))
                .mapToPair(str -> new Tuple2<String, Integer>(str, 1))
                .reduceByKey((val1, val2) -> val1+val2)
                .mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1))
                .sortByKey(false)   //descending
                .collect()
                .forEach(tuple -> System.out.println("Keywords : " + tuple._2 + " || Count: " + tuple._1));

    }
}
