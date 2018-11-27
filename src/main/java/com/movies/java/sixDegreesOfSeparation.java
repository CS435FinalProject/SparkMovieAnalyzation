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

    static String crewDataFile  = "src/main/resources/actors";
    static String titleDataFile = "src/main/resources/movies";
    private static final Pattern TAB = Pattern.compile("\t");

    public static String sourceID;
    public static String destinationID;

    public static Dataset<Row> crewTable;
    public static Dataset<Row> titleTable;

    public static Set<String> titlesVisited;
    public static Set<String> actorsVisited;

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

    public static String getCrewID(String name) {
        Dataset<Row> row = spark.sql("SELECT id FROM global_temp.crew_T WHERE assoc LIKE '" + name + "__%'");
        Row attributes = row.collectAsList().get(0);
        return attributes.get(0).toString();
    }

    public static class Node {
        private Node parent;
        private String value;
        private boolean isActor;

        Node(Node parent, String value) {
            this.parent = parent;
            this.value = value;
            this.isActor = (parent == null) || (!parent.isActor);
        }

        public String getValue() {
            return this.value;
        }

        public String extractName(String id) {
            String tableToUse = (id.charAt(0) == 'n') ? "crew" : "title";
            Dataset<Row> row = spark.sql("SELECT assoc FROM global_temp." + tableToUse + "_T WHERE id='" + id + "'");
            String[] attributes = row.collectAsList().get(0).toString().split("__");
            return attributes[0].substring(1, attributes[0].length());
        }

        public String toString() {
            String currentName = extractName(this.value);

            if(parent == null) {
                return currentName + " was in";
            }
            else {
                if(isActor) {
                    return parent.toString() + "with " + currentName + "\n" + currentName;
                } else {
                    String extra = (parent.parent == null) ? " " : " was in ";
                    return parent.toString() + extra + currentName + " ";
                }
            }
        }

        public boolean isAnActor() {
            return this.isActor;
        }

        public int getDepth() {
            Node curr = this;
            int i = 0;
            while (curr.parent != null) {
                i++;
                curr = curr.parent;
            }
            return i;
        }
    }


    public static ArrayList<Node> getChildren(Node parent) {
        String whichTable = (parent.getValue().charAt(0) == 'n') ? "crew" : "title";
        Dataset<Row> row = spark.sql("SELECT assoc FROM global_temp." + whichTable + "_T WHERE id='" + parent.getValue() + "'");

        String[] parts = row.collectAsList().get(0).toString().split("__");
        String[] associationsArray = parts[1].split(",");
        ArrayList<Node> associationsList = new ArrayList<>();

        for (String each : associationsArray) {
            if (each.charAt(each.length() - 1) == ']') {
                each = each.substring(0, each.length() - 1);
            }
            associationsList.add(new Node(parent, each));
        }

        return associationsList;
    }

    public static Node bfs() {
        Node root = new Node(null, sourceID);

        int depth = 0;
        Queue<Node> nodes = new LinkedList<>();
        actorsVisited.add(root.getValue());
        nodes.offer(root);

        while(!nodes.isEmpty() && depth < 13) {
            Node node = nodes.poll();
            for(Node n : getChildren(node)) {
                if (n.getValue().equals(destinationID)) {
                    return n;
                }
                if(n.isAnActor() && !actorsVisited.contains(n.getValue())) {
                    actorsVisited.add(n.getValue());
                    nodes.add(n);
                } else if(!n.isAnActor() && !titlesVisited.contains(n.getValue())) {
                    titlesVisited.add(n.getValue());
                    nodes.add(n);
                }
            }
            depth++;
        }
        return null;
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

        crewTable = spark.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        crewTable.createOrReplaceGlobalTempView("crew_T");

        titleTable = spark.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        titleTable.createOrReplaceGlobalTempView("title_T");

        sourceID = getCrewID(args[0]);
        destinationID = getCrewID(args[1]);

        // Run BFS
        Node path = bfs();
        if(path != null) {
            System.out.println(path.toString() + " was found in " + path.getDepth()/2 + " people!");
        }else {
            System.out.println("Cant find path");
        }

//        Run DFS
//        ArrayList<String> path = dfs(1, sourceID);
//        System.out.println("Found the path in " + path.size() + " vertices (including movies).");
//        String previous = sourceID;
//
//        for (int i = 0; i < path.size(); ++i) {
//            System.out.println(previous + " was in " + path.get(i) + " with " + path.get(++i));
//            previous = path.get(i);
//        }
    }
}