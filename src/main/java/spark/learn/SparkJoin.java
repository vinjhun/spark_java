package spark.learn;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.Optional;

public class SparkJoin {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("spark_config").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> visitors = new ArrayList<>();
        visitors.add(new Tuple2<>("4", 18));
        visitors.add(new Tuple2<>("6", 4));
        visitors.add(new Tuple2<>("10", 9));

        List<Tuple2<String, String>> userDetails = new ArrayList<>();
        userDetails.add(new Tuple2<>("1", "John"));
        userDetails.add(new Tuple2<>("2", "Bob"));
        userDetails.add(new Tuple2<>("3", "Alan"));
        userDetails.add(new Tuple2<>("4", "Doris"));
        userDetails.add(new Tuple2<>("5", "Marybelle"));
        userDetails.add(new Tuple2<>("6", "Raquel"));

        JavaPairRDD visitorsPair = sc.parallelizePairs(visitors);
        JavaPairRDD userPair = sc.parallelizePairs(userDetails);

        System.out.println("=====Inner Join");
        JavaPairRDD<String, Tuple2<Integer, String>> joinPair = visitorsPair.join(userPair);
        joinPair.collect().forEach(System.out::println); //called when confirmed is done

        System.out.println();
        System.out.println();
        System.out.println("=====Left Join ==== Optional is Used");
        //lol it was said to be ugly in specify the class
        //the optional is spark one, not java.util
        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftJoin = visitorsPair.leftOuterJoin(userPair);
        leftJoin.collect().forEach(t -> System.out.println(t._2._2.orElse("").toUpperCase()));

        System.out.println();
        System.out.println();
        System.out.println("=====Right Join ==== Optional is Used");
        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightJoin = visitorsPair.rightOuterJoin(userPair);
        rightJoin.collect().forEach(System.out::println);
        //rightJoin.collect().forEach(t -> {
        //    System.out.println("user "+ t._2._2+ " has " + t._2._1.orElse(0) + " visits");
        //});

        System.out.println();
        System.out.println();
        System.out.println("=====Full Outer Join ==== Cartesian");
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<String, String>>  fullJoin = visitorsPair.cartesian(userPair);
        fullJoin.collect().forEach(System.out::println);

        sc.close();
    }
}