package com.movies.java;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.commons.lang.ArrayUtils;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

public class sixDegreesOfSeparation {
    public static SparkContext spark;
    public static SQLContext sparksql;
    private static String hdfs = "hdfs://raleigh:30101";
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
        return spark.textFile(whichFile,10).toJavaRDD().
                mapToPair( s -> {
                    s = s.replaceAll("[()\\[\\]]", "");
                    String[] parts = TAB.split(s);
                    int idOrder = (whichFile.equals(crewDataFile)) ? 0 : 1;
                    int nameOrder = (whichFile.equals(crewDataFile)) ? 1 : 0;
                    String name = parts[nameOrder];  //changed name and idorder
                    String id   = parts[idOrder];   //swaped nameorder and idOrder

                    String[] associations = parts[2].split(", ");
                    String nameIDs = name + "__" + associations[0]; // There's at least one
                    for(int i = 1; i < associations.length; ++i) {
                        nameIDs += "," + associations[i];
                    }

                    return new Tuple2<>(id, nameIDs);
                });
    }

    public static String getCrewID(String name) {
        Dataset<Row> row = sparksql.sql("SELECT id FROM global_temp.crew_T WHERE assoc LIKE '" + name + "__%'");
        Row attributes = row.collectAsList().get(0);
        return attributes.get(0).toString();
    }

    public static class Node {
        private Node parent;
        private String value;
        private boolean isActor;
        private int depth;

        Node(Node parent, String value) {
            this.parent = parent;
            this.value = value;
            this.isActor = (parent == null) || (!parent.isActor);
            if(this.parent == null) depth = 0;
            else depth = this.parent.depth+1;
        }

        public int myDepth() { return this.depth; }

        public String getValue() {
            return this.value;
        }

        public String extractName(String id) {
            String tableToUse = (id.charAt(0) == 'n') ? "crew" : "title";
            Dataset<Row> row = sparksql.sql("SELECT assoc FROM global_temp." + tableToUse + "_T WHERE id='" + id + "'");
            List<Row> rowl = row.collectAsList();
            if(rowl.size() == 0 ) return "";
            String[] attributes = rowl.get(0).toString().split("__");
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
        Dataset<Row> row = sparksql.sql("SELECT assoc FROM global_temp." + whichTable + "_T WHERE id='" + parent.getValue() + "'");
        List<Row> rowl = row.collectAsList();
        if(rowl.size() == 0) return new ArrayList<Node>();
        String[] parts = rowl.get(0).toString().split("__");
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

        while(!nodes.isEmpty()) {
            Node node = nodes.poll();
            if(node.myDepth() >= 13) break;
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
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        if(args.length < 2){
            System.out.println("USAGE: sixDegreesOfSeparation <nameOfFromPerson> <nameOfToPerson>");
        }
        titleDataFile = hdfs+args[0];
        crewDataFile = hdfs+args[1];
        titlesVisited = Collections.synchronizedSet(new HashSet<String>(5430168, (float) 1.0));
        actorsVisited = Collections.synchronizedSet(new HashSet<String>(8977203, (float) 1.0));

//        spark = SparkSession
//                .builder()
//                .appName("Page Rank With Taxation")
//                ;
        spark = new SparkContext(new SparkConf().setAppName("Find"));
        sparksql = new SQLContext(spark);

        JavaPairRDD<String, String> crewLines = makeRDD(crewDataFile);
        JavaPairRDD<String, String> titleLines = makeRDD(titleDataFile);

        crewTable = sparksql.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
        crewTable.createOrReplaceGlobalTempView("crew_T");

        titleTable = sparksql.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
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
//        spark.
//        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));
//        writer.write(output);
//        writer.close();

//        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
//
////Get configuration of Hadoop system
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfs);
        System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        //System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        FileSystem fs = FileSystem.get(URI.create(hdfs), conf);

        Path hdfswritepath = new Path(output + "/" + "results.txt");
        FSDataOutputStream outputStream=fs.create(hdfswritepath);
        //Cassical output stream usage
        outputStream.writeBytes(output);
        outputStream.close();
//
////Destination file in HDFS
//        FileSystem fs = FileSystem.get(URI.create(dst), conf);
//        OutputStream out = fs.create(new Path(dst));
//
////Copy file from local to HDFS
//        IOUtils.copyBytes(in, out, 4096, true);

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
    //The Long Night Brenda Cowling Glyn Houston Betty McDowall
}