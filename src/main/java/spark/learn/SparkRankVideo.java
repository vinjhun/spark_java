package spark.learn;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

//do by myself
public class SparkRankVideo {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //dun set master upon running inside apache emr
        SparkConf config = new SparkConf().setAppName("spark_video_view").setMaster("local[*]");

        //set test mode, usually useful in testing
        Boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewRDD = obtainViewData(config, testMode);
        JavaPairRDD<Integer, Integer> chapterRDD = obtainChapterData(config, testMode);
        JavaPairRDD<Integer, String> titleRDD = obtainTitlesData(config, testMode);

        //even using right outer join also will use the first key in tuple, required to reverse
        //BOTH COMPARE WITH TUPLE1
        //view left outer join view._1 match with chapter._1 ( left on view)
        //right outer join also compare view._1 with chapter._1 ( but right on chapter )
        //join occurred on first key
        viewRDD.leftOuterJoin(chapterRDD)
                .collect()
                .forEach(e -> {
                    /*
                    System.out.println("====Test");
                    System.out.println("View: " + e._2._1);
                    System.out.println("Chapter: " + e._1);
                    System.out.println("Course:  " + e._2._2.orElse(-1));
                     */
                });

        //exercise 1: course id and the number of chapter
        //chap id - course id
        chapterRDD.mapToPair(e -> new Tuple2<>(e._2, 1)).reduceByKey((c1, c2) -> c1 + c2)
                .collect().forEach(e -> {
                    /*
                    System.out.println("====Test");
                    System.out.println("Course : " + e._1);
                    System.out.println("Num of Chapters : " + e._2);
                    */
                });

        //exercise 2: most popular courses by scores, (views?)
        //1)user id, chapter id -> chapter id, user id
        //2) chapter id, and course id
        //3) chapter id, user id, optional(course id)
        //totally wrong hahhahaha (we should get percentage instead of the max attended course)
        viewRDD.mapToPair(e -> new Tuple2<>(e._2, e._1))
                .distinct()
                .leftOuterJoin(chapterRDD)
                .filter(missingChapter -> missingChapter._2._2.isPresent()) // still correct here
                .mapToPair(e -> new Tuple2<>(e._2._2.get(), 1))// here wrong edy..
                .reduceByKey((count1, count2) -> count1 + count2)
                .mapToPair( e-> new Tuple2<>(e._2, e._1))
                .sortByKey(false)
                .mapToPair( e-> new Tuple2<>(e._2, e._1))
                .collect().forEach(e -> {
                    /*
                    System.out.println("=====Exercise 2");
                    System.out.println("Course : " + e._1);
                    System.out.println("User Participated : " + e._2);
                     */
                });


        //assume on the chapters.csv is the configuration of each course has N chapters
        //and titles also is the configuration
        //chapter id, user id, optional(course id)
        //example given use user as a key to separate difference course:
        //like user 1 has view course 1 2 times
        // user 2 has view course 1 1 times , and break into percentage
        //steps 6 1 2 , 1 1 , 2 1 , get hold of 1 2,3  1 1,3
        JavaPairRDD<Integer, Integer> step6 = viewRDD.mapToPair(e -> new Tuple2<>(e._2, e._1))
                .distinct()
                .leftOuterJoin(chapterRDD)
                .filter(missingChapter -> missingChapter._2._2.isPresent())
                .mapToPair(e -> new Tuple2<>(e._2, 1))
                .reduceByKey((count1, count2) -> count1 + count2)   //view
                .mapToPair(e -> new Tuple2<Integer, Integer>(e._1._2.get(), e._2));    //here required get hold of total.. steps 6, hard!

        // 1 2 , 1 1 --> 1 (2,3) 1 (1,3)
        //group into a list
        //wrong again, thought is within the obtained value, but throughout whole table of chapters
        //JavaPairRDD<Integer, Integer> sumTable = step6.reduceByKey((c1, c2) -> c1 + c2);
        JavaPairRDD<Integer, Integer> sumTable = chapterRDD.mapToPair(chapter -> new Tuple2<>(chapter._2, 1))
                        .reduceByKey((c1,c2) -> c1+c2);

        //when left join, 1 (2,3) 1 (1,3)
        //can we use ... big decimal?
        JavaPairRDD<Integer, Integer> result = step6.leftOuterJoin(sumTable)
                .mapToPair(tuple2 -> new Tuple2<Integer, Double>(tuple2._1,
                        tuple2._2._1.doubleValue() / tuple2._2._2.get().doubleValue()))
                .mapToPair(tuple -> {
                    //obtain scores
                    Double percentage = tuple._2 * 100;
                    Integer score = 0;

                    if (percentage > 90) {
                        score = 10;
                    } else if (percentage > 50) {
                        score = 4;
                    } else if (percentage > 25) {
                        score = 2;
                    } else {
                        score = 0;
                    }

                    return new Tuple2<>(tuple._1 , score);
                });

        //exercise 3 - get the title using join, sort scores by descending
        // course id , score title
        //score, (course id | title)
        //check on score all greater than 10?
        result.leftOuterJoin(titleRDD)
                .mapToPair(scoreKey -> {
                    Tuple2<Integer, String> inner = new Tuple2<>(scoreKey._1, scoreKey._2._2.orElse("N/A"));
                    return new Tuple2<>(scoreKey._2._1, inner);
                })
                .sortByKey(false)
                .collect()
                .forEach(exercise -> {
                    System.out.println("=== Exercise 3");
                    System.out.println("Course ID : " + exercise._2._1 + " (" + "Title : " + exercise._2._2 +  ")");
                    System.out.println("Score : " + exercise._1);
                });

        //exercise 4 run using real data

    }

    private static JavaPairRDD<Integer, String> obtainTitlesData(SparkConf config, Boolean testMode) {
        //JavaSparkContext sc = new JavaSparkContext(config); unable to support multiple spark context 2243
        //multiple sc vs one sc?
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(config));

        if (testMode) {
            //courseid , chapter id
            List<Tuple2<Integer, String>> testDataList = new ArrayList<>();
            testDataList.add(new Tuple2<Integer, String>(1,"HTML5"));
            testDataList.add(new Tuple2<Integer, String>(2,"Java Fundamentals"));
            testDataList.add(new Tuple2<Integer, String>(14,"Spring Remoting and Webservices"));
            testDataList.add(new Tuple2<Integer, String>(6,"Spring Security Module 3"));
            testDataList.add(new Tuple2<Integer, String>(18,"Microservice Deployment"));
            testDataList.add(new Tuple2<Integer, String>(12,"Cloud Deployment"));
            testDataList.add(new Tuple2<Integer, String>(10,"Thymeleaf"));


            return sc.parallelizePairs(testDataList);
        }


        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(e -> {
                   String[] commaSeparated = e.split(",");
                   return new Tuple2<Integer, String>(Integer.valueOf(commaSeparated[0]), commaSeparated[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> obtainChapterData(SparkConf config, Boolean testMode) {
        //JavaSparkContext sc = new JavaSparkContext(config);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(config));

        if (testMode) {
            //chapter id, course id
            List<Tuple2<Integer, Integer>> testDataList = new ArrayList<>();
            testDataList.add(new Tuple2<>(96,  1));
            testDataList.add(new Tuple2<>(97,  1));
            testDataList.add(new Tuple2<>(98,  1));
            testDataList.add(new Tuple2<>(99,  2));
            testDataList.add(new Tuple2<>(100, 3));
            testDataList.add(new Tuple2<>(101, 3));
            testDataList.add(new Tuple2<>(102, 3));
            testDataList.add(new Tuple2<>(103, 3));
            testDataList.add(new Tuple2<>(104, 3));
            testDataList.add(new Tuple2<>(105, 3));
            testDataList.add(new Tuple2<>(106, 3));
            testDataList.add(new Tuple2<>(107, 3));
            testDataList.add(new Tuple2<>(108, 3));
            testDataList.add(new Tuple2<>(109, 3));

            return sc.parallelizePairs(testDataList);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(e -> {
                    String[] commaSeparated = e.split(",");
                    return new Tuple2<Integer, Integer>(Integer.valueOf(commaSeparated[0]), Integer.valueOf(commaSeparated[0]));
                });
    }

    /**
     * What if want to massage more than one data?
     * JavaPairRDD<Integer, Tuple<Integer, Tuple<Integer, Integer>>>> ?
     * **/
    private static JavaPairRDD<Integer, Integer> obtainViewData(SparkConf config, Boolean testMode) {
        //JavaSparkContext sc = new JavaSparkContext(config);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(config));

        if (testMode) {
            //user id, chapter id
            List<Tuple2<Integer, Integer>> testDataList = new ArrayList<>();
            testDataList.add(new Tuple2<>(14, 96));
            testDataList.add(new Tuple2<>(14, 97));
            testDataList.add(new Tuple2<>(13, 96));
            testDataList.add(new Tuple2<>(13, 96));
            testDataList.add(new Tuple2<>(13, 96));
            testDataList.add(new Tuple2<>(14, 99));
            testDataList.add(new Tuple2<>(13, 100));

            return sc.parallelizePairs(testDataList);
        }

        //maptoPair!!!
        return sc.textFile("src/main/resources/viewing figures/views-1.csv")
                .mapToPair(commaSeparated -> {
                    String[] columns = commaSeparated.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }


}
