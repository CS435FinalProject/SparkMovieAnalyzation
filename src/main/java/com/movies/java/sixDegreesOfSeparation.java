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

public class sixDegreesOfSeparation {
    public static SparkSession spark;
    public static final ArrayList<String> EMPTY = new ArrayList<>();

    static String crewDataFile  = "src/main/resources/actors";
    static String titleDataFile = "src/main/resources/movies";
    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern TAB = Pattern.compile("\t");

    public static String sourceID;
    public static String destinationID;

    public static Dataset<Row> crewTable;
    public static Dataset<Row> titleTable;

    public static Set<String> titlesVisited;
    public static Set<String> actorsVisited;

    public static ArrayList<String> getAssociations(String id) {
        Dataset<Row> row = null;
        ArrayList<String> associationsList = new ArrayList<>();

        if (id.charAt(0) == 'n') {
            if (actorsVisited.contains(id)) {
                return EMPTY; // dead end
            } else {
                synchronized (actorsVisited) {
                    actorsVisited.add(id);
                }
            }
            row = spark.sql("SELECT assoc FROM global_temp.crew_T WHERE id='" + id + "'");

        } else if (id.charAt(0) == 't') {
            if (titlesVisited.contains(id)) {
                return EMPTY; // dead end
            } else {
                synchronized (titlesVisited) {
                    titlesVisited.add(id);
                }
            }
            row = spark.sql("SELECT assoc FROM global_temp.title_T WHERE id='" + id + "'");
        }
//        System.out.println(id);
        String[] parts = row.collectAsList().get(0).toString().split("__");
        String[] associationsArray = parts[1].split(",");

        for (String each : associationsArray) {
            if (each.charAt(each.length() - 1) == ']') {
                each = each.substring(0, each.length() - 1);
            }
//            System.out.println(id + " is associated to " + each);
            associationsList.add(each);
        }

        return associationsList;
    }

    public static String getCrewID(String name) {
        Dataset<Row> row = spark.sql("SELECT id FROM global_temp.crew_T WHERE assoc LIKE '" + name + "__%'");
        Row attributes = row.collectAsList().get(0);
        return attributes.get(0).toString();
    }

    public static ArrayList<String> dfs(int depth, String crewID) {
        if (depth > 6) {
            return EMPTY; // depth = 7
        }

//        System.out.println("depth " + depth + ": " + crewID);
        ArrayList<String> movies = getAssociations(crewID);

        HashMap<String, ArrayList<String>> movieActors = new HashMap<>();

        for (String movieID : movies) {
            // if (movieID.equals(destinationID)) { path.add(movie); return path; }
            ArrayList<String> crewAlsoInMovie = getAssociations(movieID);

            for (int i = 0; i < crewAlsoInMovie.size(); ++i) {
                String currentCrewAlsoInMovie = crewAlsoInMovie.get(i);
                if (currentCrewAlsoInMovie.equals(destinationID)) {
                    ArrayList<String> path = new ArrayList<>();
                    path.add(movieID);
                    path.add(currentCrewAlsoInMovie);
                    return path;
                } else if (currentCrewAlsoInMovie.equals(crewID)) {
//                    System.out.println("Removing self (" + crewID + ") from " + movieID);
                    crewAlsoInMovie.remove(i--);
                }
            }
            movieActors.put(movieID, crewAlsoInMovie);
        }

        for (int i = 0; i < movies.size(); ++i) {
            String currentMovie = movies.get(i);
            ArrayList<String> crew = movieActors.get(currentMovie);
            for (String currentCrew : crew) {
                ArrayList<String> path = dfs(depth + 1, currentCrew);

                if (!path.isEmpty()) {

                    if (path.size() >= 2) {
                        path.add(0, currentCrew);
                        path.add(0, currentMovie);
                        return path;
                    }
                    return path;
                }
            }
            movieActors.remove(currentMovie);
        }

        return EMPTY; // Did not find
    }

    public static JavaPairRDD<String, String> makeRDD(String whichFile) {
        return spark.read().textFile(whichFile).javaRDD().
                mapToPair( s -> {
                    s = s.replaceAll("[()\\[\\]]", "");
                    String[] parts = TAB.split(s);
                    String name = parts[0];
                    String id   = parts[1];

                    String[] associations = parts[2].split(", ");
                    String nameIDs = name + "__" + associations[0]; // There's at least one
                    for(int i = 1; i < associations.length; ++i) {
                        nameIDs += "," + associations[i];
                    }

                    return new Tuple2<>(id, nameIDs);
                });
    }

    public static void main(String[] args) {
        if(args.length < 2){
            System.out.println("USAGE: sixDegreesOfSeparation <nameOfFromPerson> <nameOfToPerson>");
        }

        titlesVisited = Collections.synchronizedSet(new HashSet<String>(5430168, (float) 1.0));
        actorsVisited = Collections.synchronizedSet(new HashSet<String>(8977203, (float) 1.0));

        spark = SparkSession
                .builder()
                .master("local")
                .appName("Page Rank With Taxation")
                .getOrCreate();

        JavaPairRDD<String, String> crewLines = makeRDD(crewDataFile);
        JavaPairRDD<String, String> titleLines = makeRDD(titleDataFile);

        crewLines.foreach(
                s -> {
                    System.out.println(s);
                }
        );

        titleLines.foreach(
                s -> {
                    System.out.println(s);
                }
        );

        crewTable = spark.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        crewTable.createOrReplaceGlobalTempView("crew_T");
//        crewTable.show();

        titleTable = spark.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        titleTable.createOrReplaceGlobalTempView("title_T");
//        titleTable.show();

        sourceID = getCrewID(args[0]);
        destinationID = getCrewID(args[1]);
        System.out.println("Finding path from " + sourceID + " to " + destinationID);

        ArrayList<String> path = dfs(1, sourceID);

        if (path.isEmpty()) {
            System.out.println("Did not find a path ]:");
        } else {
            System.out.println("Found the path in " + path.size() + " vertices (including movies).");
            String previous = sourceID;
            for (String each : path) {
                System.out.println(each);
            }

            for (int i = 0; i < path.size(); ++i) {
                System.out.println(previous + " was in " + path.get(i) + " with " + path.get(++i));
                previous = path.get(i);
            }
        }
    }
}