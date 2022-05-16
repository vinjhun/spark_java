package spark.learn;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class SparkMain {

    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(1.22);
        inputData.add(2.44);
        inputData.add(312312.222);
        inputData.add(2223.112);
        inputData.add(909.888);

        //warning only (must be apache library not java.util
        //hadoop dependency?
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //spark creation
        //name will appear in report
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        //connection to spark cluster
        JavaSparkContext sc = new JavaSparkContext(conf);

        //load a collections parallelize into rdd
        //and rdd is a wrapper towards scala
        JavaRDD<Double> firstRdd = sc.parallelize(inputData);
        //filter by value which more than 10
        JavaRDD<Double> moreThanTen = firstRdd.filter(value -> value > 10);

        //====Reduce
        System.out.println();
        System.out.println();
        Double sum = firstRdd.reduce((double1, double2) -> double1 + double2);
        Double sumMtTen = moreThanTen.reduce((double1, double2) -> double1 + double2);

        System.out.println("===Reduce===");
        System.out.println("All the Sum : " + sum);
        System.out.println("Sum Except Less Than 10 : " + sumMtTen);


        //====Mapping
        System.out.println();
        System.out.println();
        JavaRDD<Integer> sqrtRdd = firstRdd.map(value -> new Double(Math.sqrt(value)).intValue());
        System.out.println("===Mapping===");
        sqrtRdd.collect().forEach(System.out::println);

        //the foreach required Serializable value, but the Integer is not serializable?
        //Required to Serialize in order to send across multiple cluster?
        //sqrtRdd.foreach();

        //====Count
        System.out.println();
        System.out.println();
        Integer countMpReduce = firstRdd.map(v1 -> 1).reduce((v1, v2) -> v1 + v2);
        Long normalCount = firstRdd.count();
        System.out.println("===Count===");
        System.out.println("Count Map Reduce : " + countMpReduce);
        System.out.println("NormalCount : " + normalCount);

        //====Object


        //===Tuple (Scala Concept)
        System.out.println();
        System.out.println();
        List<Integer> data = new ArrayList<>();
        data.add(35);
        data.add(12);
        data.add(14);
        data.add(22);

        JavaRDD<Integer> list = sc.parallelize(data);
        JavaRDD<Tuple2> tupleRdd = list.map(value -> new Tuple2(value, Math.sqrt(value)));


        //===PairRDD
        //===PairRDD key can be duplicate
        //how to initialize pairrdd -> Tuple2!!!!
        //beware of grouping by key ( might cause performance issue) as there are many cluster
        System.out.println();
        System.out.println();
        List<String> msg = new ArrayList<>();
        msg.add("WARN: 22/05/15 16:14:31");
        msg.add("ERROR: 22/05/15 16:14:31");
        msg.add("FATAL: 22/05/15 16:14:31");
        msg.add("WARN: 22/05/15 16:14:31");

        System.out.println("===PairRDD===");
        sc.parallelize(msg)
                .mapToPair(value -> new Tuple2<String, Long>(String.valueOf(value.split(":")[0]), 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .collect().forEach(tuple -> System.out.println("Message " + tuple._1 + " has " + tuple._2 + "count"));

        //===FlatMap
        System.out.println();
        System.out.println();
        System.out.println("===Flat Map===");

        JavaRDD<String> individualWords = sc.parallelize(msg).flatMap(value -> Arrays.asList(value.split(" ")).iterator());

        //====boring sorting
        /**
         System.out.println("Sorting in RDD");
         firstRdd.sortBy((value) -> value,true , Integer.MAX_VALUE).collect().forEach(System.out::println);

         System.out.println("Not Sorting");
         firstRdd.collect().forEach(System.out::println);
         **/

        sc.close(); //context is required to close
    }
}
