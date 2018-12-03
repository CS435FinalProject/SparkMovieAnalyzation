package com.movies.java;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;


public class SparkDegreesOfSeperation {
    public static SparkSession spark;
    public static String dataDirectory;
    private static final Pattern TAB = Pattern.compile("\t");
    public static Node result = new Node(null,"","", new LinkedList<>(), 0, 0);
    public static String sourceName;
    public static String destinationName;

    public static JavaPairRDD<String, Node> rdd;
    public static Node found;

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

                    Node newNode = new Node(null, name, id, associations, 0, 0);

                    return new Tuple2<>(id, newNode);
                });
    }

    public static JavaPairRDD<String, Node> filterRDD(String filterBy, boolean isActor) {
        return rdd.filter(
                (id_Node) -> id_Node._2.getName().equals(filterBy)
        ).flatMapToPair(x -> {
            List<Tuple2<String, Node>> nodes = new ArrayList<>();
            for(String s: x._2.getAssociations()){
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

    }

    public static class Node implements Serializable{
        private Node parent;
        private String name;
        private String id;
        private int distance;
        private int status;
//        private boolean isActor;
        private LinkedList<String> associations;
//        private int depth;
//        private Node target;
        private String find;


        Node(Node parent, String name, String id, LinkedList<String> associations, int status, int distance) {
            this.associations = associations;
            this.parent = parent;
//            this.isActor = (parent == null) || (!parent.isActor);
            this.name = name;
            this.id = id;
            this.status = status;
            this.distance = distance;
        }
        Node(Node parent, String name, String id, LinkedList<String> associations, int status, int distance, String find) {
            this(parent,name,id,associations,status,distance);
            this.find = find;
        }

        public Iterator<Tuple2<String, Node>> flatMapNode() {
            List<Tuple2<String, Node>> list = new ArrayList<>();
            list.add(new Tuple2<>(this.id,this));
            if(status == 1) {
                if(id.equals(find)) {
                    System.out.println(this.toStringRecur(true,true));
//                    longAccumulator.add(1);
                    return list.iterator();
                }
                for (String assoc : associations) {
                    list.add(new Tuple2<>(assoc,new Node(this, "",assoc,new LinkedList<>(),1,this.distance+1,this.find)));
                }
                this.status = 2;
            }
            return list.iterator();
        }

        public String getName() {
            return this.name;
        }

        public String getID() {
            return this.id;
        }

        public int getDistance() { return this.distance; }

        public int getStatus() { return this.status; }

        public void setStatus(int status) { this.status = status; }

        public Node getParent() { return this.parent; }



        public void setFind(String find) {this.find = find; }

        public String getFind() { return this.find; }

        public String toString() {
            String temp = "";
            if(parent != null) temp = parent.name;
            return "Name: " + this.name + " Id: " + this.id + " Status: " + status + " depth: " + distance + " FIND: " + find + " Parent: " + temp;
        }

        public String toStringRecur(boolean isActor, boolean first) {
            if(this.parent != null) {
                if(isActor) {
                    String temp = "";
                    if(first) temp += " was found in " + distance / 2 + " steps\n";
                    return parent.toStringRecur(!isActor, false) + " with " + this.name +"\n" + this.name + temp;
                }else {
                    return parent.toStringRecur(!isActor, false) + " was in " + this.name;
                }
            }else {
                return this.name;
            }
        }

        public LinkedList<String> getAssociations() { return this.associations; }
    }


    public static Node reduceNode(Node node1, Node node2) {
        LinkedList<String> associations;
        if(node1.getAssociations().size() > node2.getAssociations().size()) associations = node1.getAssociations();
        else associations = node2.getAssociations();
        int distance = Math.min(node1.getDistance(), node2.getDistance());
        if(node1.getParent() == null || node2.getParent() == null) distance = Math.max(node1.getDistance(), node2.getDistance());
        int status = Math.max(node1.getStatus(), node2.getStatus());
        Node parent;
        if(node1.getParent() != null && node2.getParent() == null) parent = node1.getParent();
        else if(node2.getParent() != null && node1.getParent() == null) parent = node2.getParent();
        else {
            if(node1.getDistance() < node2.getDistance()) parent = node1.getParent();
            else parent = node2.getParent();
        }
        String name;
        if(node2.getName().length() > node1.getName().length()) name = node2.getName();
        else name = node1.getName();

        return new Node(parent, name, node1.getID(),associations,status,distance, node1.getFind());
    }


    public static void BFS(String id) {
        for(int i = 0; i < 13; i++) {
            rdd = rdd.flatMapToPair(x -> x._2.flatMapNode()).reduceByKey(SparkDegreesOfSeperation::reduceNode);
            found = rdd.filter(v -> v._1.equals(id)).first()._2;
            //System.out.println(found.toString());
            if(found.getParent() != null) break;

        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 3){
            System.out.println("USAGE: SparkDegreesOfSeperation <movie/actor input files> <person> <movie> <person> <movie>");
        }
        dataDirectory = args[0];
        sourceName    = args[1];
        String sourceMovie = args[2];
        destinationName = args[3];
        String destinationMovie = args[4];

        SparkContext context;
        if(args.length > 5 && args[5].equals("true")) {
            context = new SparkContext(new SparkConf());
            spark = SparkSession
                    .builder().sparkContext(context)
                    .appName("Six Degrees of Kevin Bacon")
                    .getOrCreate();
        }else {
            context = new SparkContext(new SparkConf().setMaster("local").setAppName("Six Degrees"));
            spark = SparkSession
                    .builder().sparkContext(context)
                    .master("local")
                    .appName("Six Degrees of Kevin Bacon")
                    .getOrCreate();
        }


        rdd = makeRDD(dataDirectory);


        //rdd.foreach(s -> {System.out.println(s._1 + " " + s._2.toString());});
        Node sourceNode = getCrewID(sourceName, sourceMovie);
        System.out.println(sourceNode);

        Node destinationNode = getCrewID(destinationName, destinationMovie);
        System.out.println(destinationNode);

        rdd = rdd.mapValues(s -> {
            if(s.getID().equals(sourceNode.getID())) s.setStatus(1);
            s.setFind(destinationNode.getID());
            return s;
        });

        BFS(destinationNode.getID());

        if(found.getParent() != null) {
            System.out.println(found.toStringRecur(true, true));
        }else {
            System.out.println("No Path Found!");
        }

        if(args.length > 6 ) {
			Configuration hconf = new Configuration();
			hconf.set("fs.defaultFS", args[6]);
			System.out.println("Connecting to -- " + hconf.get("fs.defaultFS"));

			hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

			System.setProperty("hadoop.home.dir", "/");
			FileSystem fs = FileSystem.get(URI.create(args[6]), hconf);

			Path hdfswritepath = new Path(args[6] + "/" + sourceName.replaceAll("\\s+","") + "_"
                    + destinationName.replaceAll("\\s+","") + ".txt");
			FSDataOutputStream outputStream = fs.create(hdfswritepath);
			if(found.getParent() != null) outputStream.writeBytes(found.toStringRecur(true,true));
			else outputStream.writeBytes("No Connections Found");
			outputStream.close();
		}

    }
}