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

public class SparkDegreesOfSeperation {
    public static SparkSession spark;
    private static String hdfs = "";
    static String crewDataFile  = "src/main/resources/actors";
    static String titleDataFile = "src/main/resources/movies";
    private static final Pattern TAB = Pattern.compile("\t");

    public static String sourceID;
    public static String destinationID;

    public static Dataset<Row> crewTable;
    public static Dataset<Row> titleTable;

    public static Map<String, String> titlesVisited;
    public static Map<String, String> actorsVisited;

    public static JavaPairRDD<String, String> makeRDD(String whichFile, boolean isTitleFile) {
        return spark.read().textFile(whichFile).javaRDD().
                mapToPair( s -> {
                    String name;
                    String id;
                    s = s.replaceAll("[()\\[\\]]", "");
                    String[] parts = TAB.split(s);
                    if(isTitleFile) {
                        name = parts[0];
                        id   = parts[1];
                    }
                    else{
                        name = parts[1];
                        id   = parts[0];
                    }
                    String[] associations = parts[2].split(", ");
                    String nameIDs = name + "__" + associations[0]; // There's at least one
                    for(int i = 1; i < associations.length; ++i) {
                        nameIDs += "," + associations[i];
                    }
                    return new Tuple2<>(id, nameIDs);
                });
    }

    public static String getCrewID(String name, String movie) {
//        System.out.println("Looking for actor "+name+" from "+movie);
        Dataset<Row> movieRow = spark.sql("SELECT id from global_temp.title_T WHERE assoc LIKE '" + movie + "\\_\\_%'");
        List<Row> rowList= movieRow.collectAsList();
        String searchString = "";
        for(Row r : rowList){
            searchString+="LIKE '"+name+"\\_\\_%"+r.toString().replaceAll("[\\[\\]]", "")+"%' OR assoc ";
        }
        searchString = searchString.substring(0, searchString.length()-10);
//        System.out.println(searchString);
        Dataset<Row> row = spark.sql("SELECT id FROM global_temp.crew_T WHERE assoc "+searchString);
        Row attributes = row.collectAsList().get(0);
        return attributes.get(0).toString();
    }

    public static class Node {
        private Node parent;
        private String value;
        private boolean isActor;
        private String name;

        Node(Node parent, String value, String name) {
            this.parent = parent;
            this.value = value;
            this.isActor = (parent == null) || (!parent.isActor);
            this.name = name;
        }

        public String getValue() {
            return this.value;
        }

        public String extractName(String id) {
//            System.out.println("In extract name, id is "+id);

            if(actorsVisited.containsKey(id)){
                return actorsVisited.get(id);
            }
            else if(titlesVisited.containsKey(id)){
                return titlesVisited.get(id);
            }
            String tableToUse = (id.charAt(0) == 'n') ? "crew" : "title";
            Dataset<Row> row = spark.sql("SELECT assoc FROM global_temp." + tableToUse + "_T WHERE id='" + id + "'");
            List<Row> rowList = row.collectAsList();
            if(rowList.size() == 0)
                return new String();
            String[] attributes = rowList.get(0).toString().split("__");

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
//        System.out.println("In getChildren: "+parent.getValue());
        String whichTable = (parent.getValue().charAt(0) == 'n') ? "crew" : "title";
//        System.out.println("Choosing from table "+whichTable);
        Dataset<Row> row = spark.sql("SELECT id, assoc FROM global_temp." + whichTable + "_T WHERE id='" + parent.getValue() + "'");
        List<Row> rowList = row.collectAsList();
        if(rowList.size() == 0){
            System.out.println("Query returned nothing!");
            return new ArrayList<Node>();
        }
        String id = rowList.get(0).get(0).toString();
        String[] parts = rowList.get(0).toString().split("__");
        String[] associationsArray = parts[1].split(",");
        ArrayList<Node> associationsList = new ArrayList<>();

        for (String each : associationsArray) {
            if (each.charAt(each.length() - 1) == ']') {
                each = each.substring(0, each.length() - 1);
            }
//            System.out.println("Found associations: "+each);
            associationsList.add(new Node(parent, each, id));
        }

        return associationsList;
    }

    public static Node bfs() {
        Node root = new Node(null, sourceID, null);

        int depth = 0;
        Queue<Node> nodes = new LinkedList<>();
        actorsVisited.put(root.getValue(),root.extractName(root.getValue()));
        nodes.offer(root);
//        System.out.println("Starting node: "+root.getValue());
        while(!nodes.isEmpty() && depth < 13) {
//            System.out.println("Nodes queue: "+nodes.toString());
            Node node = nodes.poll();
//            System.out.println("Current node: "+node.getValue());
            for(Node n : getChildren(node)){
//                System.out.println("Visiting node: "+n.getValue());
//                System.out.println("Children are: "+getChildren(n));
                if (n.getValue().equals(destinationID)) {
                    System.out.println("Found final node!: "+n.getValue());
                    return n;
                }
                if(n.isAnActor() && !actorsVisited.containsKey(n.getValue())) {
//                    System.out.println("    Visiting actor");
                    actorsVisited.put(n.getValue(), n.name);
                    nodes.add(n);
//                    System.out.println("Added "+n.getValue()+" to queue");
                }
                else if(!n.isAnActor() && !titlesVisited.containsKey(n.getValue())) {
//                    System.out.println("    Visiting movie");
                    titlesVisited.put(n.getValue(), n.name);
                    nodes.add(n);
//                    System.out.println("Added "+n.getValue()+" to queue");

                }
            }
//            System.out.println("Depth is incrementing from "+Integer.toString(depth)+" to "+Integer.toString(depth+1));
            depth++;
//            System.out.print("At end of iteration, nodes queue is ");
            for(Node n : nodes)
                System.out.print(n.getValue()+",");
            System.out.println();

        }
        System.out.println("Exiting bfs");
        return null;
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 4){
            System.out.println("USAGE: SparkDegreesOfSeperation <nameOfFromPerson> <nameOfToPerson>");
        }
        titleDataFile = args[0];
        crewDataFile  = args[1];
        titlesVisited = Collections.synchronizedMap(new HashMap<String, String>());
        actorsVisited = Collections.synchronizedMap(new HashMap<String, String>());

        spark = SparkSession
                .builder()
//                .master("local")
                .appName("Six Degrees of Kevin Bacon")
                .getOrCreate();

        JavaPairRDD<String, String> crewLines = makeRDD(crewDataFile, false);
//        List<Tuple2<String, String>> crewLinesOut = crewLines.take(10);
//        for(Tuple2<String, String> s: crewLinesOut)
//            System.out.println(s);

        JavaPairRDD<String, String> titleLines = makeRDD(titleDataFile, true);
//        List<Tuple2<String, String>> titleLinesOut = titleLines.take(10);
//        for(Tuple2<String, String> s: titleLinesOut)
//            System.out.println(s);

        crewTable = spark.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        crewTable.createOrReplaceGlobalTempView("crew_T");

        titleTable = spark.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        titleTable.createOrReplaceGlobalTempView("title_T");

        sourceID = getCrewID(args[2], args[3]);
        System.out.println("Source ID is: "+sourceID);

        destinationID = getCrewID(args[4], args[5]);
        System.out.println("DestinationID is: "+destinationID);

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

    }
}