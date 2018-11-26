package com.movies.java;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.lang.ArrayUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.regex.Pattern;

public class sixDegreesOfSeperation {
    private static final Pattern COMMA = Pattern.compile(",");
    static String crewDataFile  = "output/joinedCrew.tsv/part-00000";
    static String titleDataFile = "output/joinedTitles.tsv/part-00000";

    public static void main(String[] args) {
        if(args.length < 2){
            System.out.println("USAGE: sixDegreesOfSeperation <nameOfFromPerson> <nameOfToPerson>");
        }

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Page Rank With Taxation")
                .getOrCreate();

        JavaPairRDD<String, Tuple2<String, Iterable<String>>> crewLines = spark.read().textFile(crewDataFile).javaRDD().
                mapToPair( s -> {
                    s = s.replaceAll("[()\\[\\]]", "");
                    String[] parts = COMMA.split(s);
                    String name = parts[0];
                    String id   = parts[1];
                    parts = (String[]) ArrayUtils.remove(parts, 0);
                    parts = (String[]) ArrayUtils.remove(parts, 0);
                    Iterable<String> titleIDs = Arrays.asList(parts);
                    return new Tuple2<>(name, new Tuple2<>(id, titleIDs));

                });


        crewLines.foreach(
                s -> {
                    System.out.println(s);
                }
        );


        JavaPairRDD<String, Tuple2<String, Iterable<String>>> titleLines = spark.read().textFile(titleDataFile).javaRDD().
                mapToPair( s -> {
                    s = s.replaceAll("[()\\[\\]]", "");
                    String[] parts = COMMA.split(s);
                    String name = parts[0];
                    String id   = parts[1];
                    parts = (String[]) ArrayUtils.remove(parts, 0);
                    parts = (String[]) ArrayUtils.remove(parts, 0);
                    Iterable<String> titleIDs = Arrays.asList(parts);
                    return new Tuple2<>(name, new Tuple2<>(id, titleIDs));

                });

        titleLines.foreach(
                s -> {
                    System.out.println(s);
                }
        );

    }
}
