package com.movies.java;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.lang.ArrayUtils;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.regex.Pattern;

public class sixDegreesOfSeperation {
    static String crewDataFile  = "output/joinedCrew.tsv/part-00000";
    static String titleDataFile = "output/joinedTitles.tsv/part-00000";
    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern TAB = Pattern.compile("\t");



    public static void main(String[] args) {
        if(args.length < 2){
            System.out.println("USAGE: sixDegreesOfSeperation <nameOfFromPerson> <nameOfToPerson>");
        }

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Page Rank With Taxation")
                .getOrCreate();

        JavaPairRDD<String, String> crewLines = spark.read().textFile(crewDataFile).javaRDD().
                mapToPair( s -> {
                    s = s.replaceAll("[()\\[\\]]", "");
                    String[] parts = TAB.split(s);
                    String name = parts[0];
                    String id   = parts[1];
                    String nameIDs = name+"__";

                    for(int i = 2; i < parts.length; i++)
                        nameIDs +=parts[i]+",";

                    return new Tuple2<>(id, nameIDs);
                });


//        crewLines.foreach(
//                s -> {
//                    System.out.println(s);
//                }
//        );


        JavaPairRDD<String,  String> titleLines = spark.read().textFile(titleDataFile).javaRDD().
                mapToPair( s -> {
                    s = s.replaceAll("[\\[\\]]", "");
                    String[] parts = TAB.split(s);
                    String name = parts[0];
                    String id   = parts[1];
                    String titleIDs = name+"__";

                    for(int i = 2; i < parts.length; i++)
                        titleIDs +=parts[i]+",";

                    return new Tuple2<>(id,  titleIDs);
                });
//
//        titleLines.foreach(
//                s -> {
//                    System.out.println(s);
//                }
//        );

        Dataset<Row> crewTable = spark.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("title","cast");
        crewTable.show();

        Dataset<Row> titleTable = spark.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("name","movies");
        titleTable.show();
    }
}
