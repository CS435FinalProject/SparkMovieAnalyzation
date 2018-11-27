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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class sixDegreesOfSeparation {
    public static SparkSession spark;
    private static String hdfs = "";
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
                    int idOrder = (whichFile.equals(crewDataFile)) ? 0 : 1;
                    int nameOrder = (whichFile.equals(crewDataFile)) ? 1 : 0;
                    String name = parts[idOrder];
                    String id   = parts[nameOrder];

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
            if(row.collectAsList().size() == 0)
                return new String();
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
        if(row.collectAsList().size() == 0)
            return new ArrayList<Node>();
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
        System.out.println("Starting node: "+root.getValue());
        while(!nodes.isEmpty() && depth < 13) {
            System.out.println("Nodes queue: "+nodes.toString());
            Node node = nodes.poll();
            System.out.println("Current node: "+node.getValue());
            for(Node n : getChildren(node)){
                System.out.println("Visiting node: "+n.getValue());
                if (n.getValue().equals(destinationID)) {
                    System.out.println("Found final node!: "+n.getValue());
                    return n;
                }
                if(n.isAnActor() && !actorsVisited.contains(n.getValue())) {
                    System.out.println("    Visiting actor");
                    actorsVisited.add(n.getValue());
                    nodes.add(n);
                }
                else if(!n.isAnActor() && !titlesVisited.contains(n.getValue())) {
                    System.out.println("    Visiting movie");
                    titlesVisited.add(n.getValue());
                    nodes.add(n);
                }
            }
            System.out.println("Depth is incrementing from "+Integer.toString(depth)+" to "+Integer.toString(depth+1));
            depth++;
        }
        System.out.println("Exiting bfs");
        return null;
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 4){
            System.out.println("USAGE: sixDegreesOfSeparation <nameOfFromPerson> <nameOfToPerson>");
        }
        titleDataFile = hdfs+args[0];
        crewDataFile = hdfs+args[1];
        titlesVisited = Collections.synchronizedSet(new HashSet<String>(5430168, (float) 1.0));
        actorsVisited = Collections.synchronizedSet(new HashSet<String>(8977203, (float) 1.0));

        spark = SparkSession
                .builder()
//                .master("local")
                .appName("Six Degrees of Kevin Bacon")
                .getOrCreate();

        JavaPairRDD<String, String> crewLines = makeRDD(crewDataFile);
        JavaPairRDD<String, String> titleLines = makeRDD(titleDataFile);

        crewTable = spark.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        crewTable.createOrReplaceGlobalTempView("crew_T");

        titleTable = spark.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        titleTable.createOrReplaceGlobalTempView("title_T");

        sourceID = getCrewID(args[2]);
        destinationID = getCrewID(args[3]);

        if (sourceID.isEmpty() || destinationID.isEmpty()) {
            System.out.println("ERROR: Could not find that person");
            return;
        }

        // Run BFS
        Node path = bfs();
        String output = "";
        if(path != null) {

            output = path.toString() + " was found in " + path.getDepth()/2 + " people!";
        }else {
            output = "Cant find path";
        }

        System.out.println(output);

//        BufferedWriter writer = new BufferedWriter(new FileWriter(args[4]));
//        writer.write(output);
//        writer.close();

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