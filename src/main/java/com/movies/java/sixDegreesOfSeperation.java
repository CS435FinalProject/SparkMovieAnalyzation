package com.movies.java;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.lang.ArrayUtils;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.regex.Pattern;

public class sixDegreesOfSeperation {
    public static SparkSession spark;

    static String crewDataFile  = "output/joinedCrew.tsv/part-00000";
    static String titleDataFile = "output/joinedTitles.tsv/part-00000";
    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern TAB = Pattern.compile("\t");



    public String sourceID;
    public String destinationID;

    public static Set<String> titles;
    public static Set<String> actors;

    public static ArrayList<String> extract(String id) {
//        Dataset<Row> row;
        String associationS;
        ArrayList<String> associations = new ArrayList<>();

        if (id.charAt(0) == 'n') {
            synchronized (actors) {
                if (actors.contains(id)) {
                    return associations; // dead end
                } else {
                    actors.add(id);
                }
            }
//            row = spark.sql("SELECT _3 FROM actorsTable WHERE _1=nm0017398");
//            associationS = actorsTable.get(id);

        } else if (id.charAt(0) == 't') {
            synchronized (titles) {
                if (titles.contains(id)) {
                    return associations; // dead end
                } else {
                    titles.add(id);
                }
            }
//            row = spark.sql("SELECT _3 FROM titlesTable WHERE _1=tt0017398");
//            associationS = titlesTable.get(id);
        }


        return associations;
    }

    public static ArrayList<String> bfs(int depth, ArrayList<String> path, String id) {
        if (depth == 6) {
            return path; // dead-end
        }

        ArrayList<String> associations = extract(id);
        return path;
    }

    public static void main(String[] args) {
        if(args.length < 2){
            System.out.println("USAGE: sixDegreesOfSeperation <nameOfFromPerson> <nameOfToPerson>");
        }
        titles = Collections.synchronizedSet(new HashSet<String>(5430168, (float) 1.0));
        actors = Collections.synchronizedSet(new HashSet<String>(5430168, (float) 1.0));

        spark = SparkSession
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
