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
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.util.StringHelper.join;

public class SparkDegreesOfSeperation {
    public static SparkSession spark;
    public static String dataDirectory;
    private static final Pattern TAB = Pattern.compile("\t");

    public static String sourceName;
    public static String destinationName;

    public static JavaPairRDD<String, Node> rdd;

    public static Map<String, String> titlesVisited;
    public static Map<String, String> actorsVisited;

    public static JavaPairRDD<String, Node> makeRDD(String file) {
        return spark.read().textFile(file).javaRDD().
                mapToPair( s -> {
                    String name;
                    String id;
                    String[] parts = TAB.split(s);
                    id       = parts[0];
                    name     = parts[1];
                    parts[2] = parts[2].substring(1,parts[2].length()-1);

                    LinkedList associations = new LinkedList(Arrays.asList(parts[2].split(", ")));
                    int status = 0;
                    if(name.equals(sourceName)) status = 1;

                    Node newNode = new Node(null, name, id, associations, status);

                    return new Tuple2<>(id, newNode);
                });
    }

    public static JavaPairRDD<String, Node> filterRDD(String filterBy, boolean isActor) {
        return rdd.filter(
                (id_Node) -> id_Node._2.getName().equals(filterBy)
        ).flatMapToPair(x -> {
            List<Tuple2<String, Node>> nodes = new ArrayList<>();
            for(String s: x._2.associations){
                if(isActor) nodes.add( new Tuple2<>(s, x._2));
                else nodes.add(new Tuple2<>(x._2.getID(), x._2));
            }
            return nodes.iterator();
        });
    }


    public static Node getCrewID(String name, String movie) {
        JavaPairRDD<String, Node> crewWithName = filterRDD(name, true);

        JavaPairRDD<String, Node> moviesWithName = filterRDD(movie, false);

        JavaPairRDD<String, Tuple2<Node,Node>> joined = crewWithName.join(moviesWithName);
        if(joined.count() > 0) {
            return joined.take(1).get(0)._2._1;
        }else return null;


//        crewWithName.foreach(s -> {System.out.println(s);});
//
//        if (crewWithName.count() == 1) {
//            Tuple2<String, Node> first = crewWithName.first();
//            finalCrew = first._2;
//        } else {
//            JavaPairRDD<String, Node> moviesWithName = rdd.filter(
//                    (id_Node) -> id_Node._2.getName().equals(name)
//            );
//            crewWithName = crewWithName.filter(x -> x._2.getID().equals(moviesWithName));
//            System.out.println("After multiple people with name=" + name + ", there are " + crewWithName.count() + " people with that name in movie=" + movie);
//            finalCrew = crewWithName.first()._2;
//        }
//        finalCrew.setDepth(0);
//        return finalCrew;
    }


    public static class Node implements Serializable {
        private Node parent;
        private String name;
        private String id;
        private int distance;
        private int status;
        private boolean isActor;
        private LinkedList<String> associations;
        private int depth;


        Node(Node parent, String name, String id, LinkedList<String> associations, int status) {
            this.associations = associations;
            this.parent = parent;
            this.isActor = (parent == null) || (!parent.isActor);
            this.name = name;
            this.id = id;
            this.status = status;
        }

        public String getName() {
            return this.name;
        }

        public String getID() {
            return this.id;
        }

        public boolean isAnActor() {
            return this.isActor;
        }

        public int getDepth() {
            return this.parent.getDepth() + 1;
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public String toString() {
            return "Name: " + this.name + " Id: " + this.id;
        }
    }

//    public static ArrayList<Node> getChildren(Node parent) {
////        System.out.println("In getChildren: "+parent.getValue());
//        String whichTable = (parent.getValue().charAt(0) == 'n') ? "crew" : "title";
////        System.out.println("Choosing from table "+whichTable);
//        Dataset<Row> row = spark.sql("SELECT id, assoc FROM global_temp." + whichTable + "_T WHERE id='" + parent.getValue() + "'");
//        List<Row> rowList = row.collectAsList();
//        if(rowList.size() == 0){
//            System.out.println("Query returned nothing!");
//            return new ArrayList<Node>();
//        }
//        String id = rowList.get(0).get(0).toString();
//        String[] parts = rowList.get(0).toString().split("__");
//        String[] associationsArray = parts[1].split(",");
//        ArrayList<Node> associationsList = new ArrayList<>();
//
//        for (String each : associationsArray) {
//            if (each.charAt(each.length() - 1) == ']') {
//                each = each.substring(0, each.length() - 1);
//            }
////            System.out.println("Found associations: "+each);
//            associationsList.add(new Node(parent, each, id));
//        }
//
//        return associationsList;
//    }

//    public static Node bfs() {
//        Node root = new Node(null, sourceID, null);
//
//        int depth = 0;
//        Queue<Node> nodes = new LinkedList<>();
//        actorsVisited.put(root.getValue(),root.extractName(root.getValue()));
//        nodes.offer(root);
////        System.out.println("Starting node: "+root.getValue());
//        while(!nodes.isEmpty() && depth < 13) {
////            System.out.println("Nodes queue: "+nodes.toString());
//            Node node = nodes.poll();
////            System.out.println("Current node: "+node.getValue());
//            for(Node n : getChildren(node)){
////                System.out.println("Visiting node: "+n.getValue());
////                System.out.println("Children are: "+getChildren(n));
//                if (n.getValue().equals(destinationID)) {
//                    System.out.println("Found final node!: "+n.getValue());
//                    return n;
//                }
//                if(n.isAnActor() && !actorsVisited.containsKey(n.getValue())) {
////                    System.out.println("    Visiting actor");
//                    actorsVisited.put(n.getValue(), n.name);
//                    nodes.add(n);
////                    System.out.println("Added "+n.getValue()+" to queue");
//                }
//                else if(!n.isAnActor() && !titlesVisited.containsKey(n.getValue())) {
////                    System.out.println("    Visiting movie");
//                    titlesVisited.put(n.getValue(), n.name);
//                    nodes.add(n);
////                    System.out.println("Added "+n.getValue()+" to queue");
//
//                }
//            }
////            System.out.println("Depth is incrementing from "+Integer.toString(depth)+" to "+Integer.toString(depth+1));
//            depth++;
////            System.out.print("At end of iteration, nodes queue is ");
////            for(Node n : nodes)
////                System.out.print(n.getValue()+",");
////            System.out.println();
//
//        }
//        System.out.println("Exiting bfs");
//        return null;
//    }

    public static void main(String[] args) throws IOException {
        if(args.length < 3){
            System.out.println("USAGE: SparkDegreesOfSeperation <movie/actor input files> <person> <movie> <person> <movie>");
        }
        dataDirectory = args[0];
        sourceName    = args[1];
        String sourceMovie = args[2];
        destinationName = args[3];
        String destinationMovie = args[4];

        titlesVisited = Collections.synchronizedMap(new HashMap<String, String>());
        actorsVisited = Collections.synchronizedMap(new HashMap<String, String>());

        spark = SparkSession
                .builder()
                .master("local")
                .appName("Six Degrees of Kevin Bacon")
                .getOrCreate();

        rdd = makeRDD(dataDirectory);
//        rdd.foreach(
//                s->{
//                    System.out.println(s);
//                }
//        );

        Node sourceNode = getCrewID(sourceName, sourceMovie);
        System.out.println(sourceNode);

        Node destinationNode = getCrewID(destinationName, destinationMovie);
        System.out.println(destinationNode);


//        JavaPairRDD<String, String> crewLines = makeRDD(crewDataFile, false);
//        List<Tuple2<String, String>> crewLinesOut = crewLines.take(10);
//        for(Tuple2<String, String> s: crewLinesOut)
//            System.out.println(s);
//
//        JavaPairRDD<String, String> titleLines = makeRDD(titleDataFile, true);
//        List<Tuple2<String, String>> titleLinesOut = titleLines.take(10);
//        for(Tuple2<String, String> s: titleLinesOut)
//            System.out.println(s);
//
//        crewTable = spark.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
//        crewTable.createOrReplaceGlobalTempView("crew_T");
//
//        titleTable = spark.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
//        titleTable.createOrReplaceGlobalTempView("title_T");

//        sourceID = getCrewID(args[2], args[3]);
//        System.out.println("Source ID is: "+sourceID);
//
//        destinationID = getCrewID(args[4], args[5]);
//        System.out.println("DestinationID is: "+destinationID);
//
//        if (sourceID.isEmpty() || destinationID.isEmpty()) {
//            System.out.println("ERROR: Could not find that person");
//            return;
//        }
//
//        // Run BFS
//        Node path = bfs();
//        String output = "";
//        if(path != null) {
//
//            output = path.toString() + " was found in " + path.getDepth()/2 + " people!";
//        }else {
//            output = "Cant find path";
//        }
//
//        System.out.println(output);

    }
}