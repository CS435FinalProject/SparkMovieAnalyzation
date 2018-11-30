//package com.movies.java;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.sql.SQLImplicits;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.*;
//import org.apache.commons.lang.ArrayUtils;
//import scala.Tuple2;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import java.io.*;
//import java.net.URI;
//import java.util.*;
//import java.util.regex.Pattern;
//
//public class YarnDegreesOfSeparation {
//	public static SparkSession sparkSession;
//	private static String hdfs = "hdfs://raleigh:30101";
////	static String crewDataFile  = "src/main/resources/actors";
////	static String titleDataFile = "src/main/resources/movies";
//	static String inputFolder ="";
//	private static final Pattern TAB = Pattern.compile("\t");
//
//	public static String sourceID = "";
//	public static String destinationID = "";
//
//	public static Dataset<Row> crewTable;
//	public static Dataset<Row> titleTable;
//
//
//	public static Map<String, String> titlesVisited;
//	public static Map<String, String> actorsVisited;
//
//	public static JavaPairRDD<String, String> makeRDD(String whichFile, boolean isCrew) {
//		return sparkSession.read().textFile(whichFile).toJavaRDD().
//				mapToPair( s -> {
//					s = s.replaceAll("[()\\[\\]]", "");
//					String[] parts = s.split("\t");
//					int idOrder;
//					int nameOrder;
//					//idOrder = (whichFile.equals(crewDataFile)) ? 0 : 1;
////					nameOrder = (whichFile.equals(crewDataFile)) ? 1 : 0;
//					if(isCrew) {
//						idOrder = 0;
//						nameOrder = 1;
//					}else {
//						idOrder = 1;
//						nameOrder = 0;
//					}
//
//					String name = parts[nameOrder];  //changed name and idorder
//					String id   = parts[idOrder];   //swaped nameorder and idOrder
//
//					if(name.equals("Lolina Zackow") || name.equals("Sam Safinia")) {
//						System.out.println("ID: " + idOrder + " name: " + nameOrder);
//						System.out.println("String: " + s);
//						System.out.println(Arrays.toString(parts));
//					}
//
//					String[] associations = parts[2].split(", ");
//					String nameIDs = name + "__" + associations[0]; // There's at least one
//					for(int i = 1; i < associations.length; ++i) {
//						nameIDs += "," + associations[i];
//					}
//					return new Tuple2<>(id, nameIDs);
//				});
//	}
//
////	public static void createHashset(String filename, boolean isCrew, String crew1, String crew2) throws IOException {
////		BufferedReader bf = new BufferedReader(new FileReader(filename));
////		String line;
////		while ((line = bf.readLine()) != null) {
////			String[] arr = line.split("\t");
////			if(isCrew) {
////				System.out.println("ARR: " + Arrays.toString(arr));
////				System.out.println("ARGS: " + crew1 + " " + crew2);
////				if(arr[1].equals(crew1)) sourceID = arr[0];
////				if(arr[1].equals(crew2)) destinationID = arr[0];
////				actors.put(arr[0],new Tuple2<>(arr[1],arr[2].split(",")));
////			}
////			else {
////				System.out.println("ARRTitle: " + Arrays.toString(arr));
////				System.out.println("ARGS: " + crew1 + " " + crew2);
////				titles.put(arr[1],new Tuple2<>(arr[0],arr[2].split(",")));
////			}
////		}
////	}
//
//
//
//	public static class Node {
//		private Node parent;
//		private String value;
//		private String name;
//		private boolean isActor;
//		private int depth;
//		private ArrayList<Node> children;
//
//		Node(Node parent, String value) {
//			this.parent = parent;
//			this.value = value;
//			this.isActor = (parent == null) || (!parent.isActor);
//			if(this.parent == null) depth = 0;
//			else depth = this.parent.depth+1;
//		}
//
//		public int myDepth() { return this.depth; }
//
//		public String getValue() {
//			return this.value;
//		}
//
//		public void setName(String name) { this.name = name;}
//		public String getName() { return this.name; }
//
//		public void setChildren(ArrayList<Node> children) { this.children = children; }
//
//		public ArrayList<Node> getChildren() { return this.children; }
//
//		public String extractName(String id) {
//			if(id.charAt(0) == 'n') {
//				if(actorsVisited.containsKey(id)) return actorsVisited.get(id);
//			}else {
//				if(titlesVisited.containsKey(id)) return titlesVisited.get(id);
//			}
//
//			String tableToUse = (id.charAt(0) == 'n') ? "crew" : "title";
//
//			Dataset<Row> row = sparkSession.sql("SELECT assoc FROM global_temp." + tableToUse + "_T WHERE id='" + id + "'");
//			List<Row> rowl = row.collectAsList();
//			if(rowl.size() == 0 ) return "";
//			String[] attributes = rowl.get(0).toString().split("__");
//			return attributes[0].substring(1, attributes[0].length());
//		}
//
//		public String toString() {
//			String currentName = name;
//
//			if(parent == null) {
//				return currentName + " was in";
//			}
//			else {
//				if(isActor) {
//					return parent.toString() + "with " + currentName + "\n" + currentName;
//				} else {
//					String extra = (parent.parent == null) ? " " : " was in ";
//					return parent.toString() + extra + currentName + " ";
//				}
//			}
//		}
//
//		public boolean isAnActor() {
//			return this.isActor;
//		}
//
//		public int getDepth() {
//			Node curr = this;
//			int i = 0;
//			while (curr.parent != null) {
//				i++;
//				curr = curr.parent;
//			}
//			return i;
//		}
//	}
//
//	public static String getCrewID(String name, String movie) {
////		Dataset<Row> row = sparkSession.sql("SELECT id FROM global_temp.crew_T WHERE assoc LIKE '" + name + "__%'");
////		Row attributes = row.collectAsList().get(0);
////		return attributes.get(0).toString();
//		System.out.println("Looking for actor "+name+" from "+movie);
//		Dataset<Row> movieRow = sparkSession.sql("SELECT id from global_temp.title_T WHERE assoc LIKE '" + movie + "\\_\\_%'");
//		List<Row> rowList= movieRow.collectAsList();
//		String searchString = "";
//		for(Row r : rowList){
//			searchString+="LIKE '"+name+"\\_\\_%"+r.toString().replaceAll("[\\[\\]]", "")+"%' OR assoc ";
//		}
//		searchString = searchString.substring(0, searchString.length()-10);
//		System.out.println(searchString);
//		Dataset<Row> row = sparkSession.sql("SELECT id FROM global_temp.crew_T WHERE assoc "+searchString);
//		Row attributes = row.collectAsList().get(0);
//		return attributes.get(0).toString();
//
//	}
//
//	public static void manyChildren(ArrayList<Node> nodes, boolean isCrew) {
//		String whichTable = "";
//		if(isCrew) whichTable = "crew";
//		else whichTable = "title";
//		String searchString = "SELECT id,assoc FROM global_temp." + whichTable + "_T WHERE id=''";
//		for(Node n : nodes) {
//			searchString += " OR id='" + n.getValue() + "'";
//		}
//		Dataset<Row> row =sparkSession.sql(searchString);
//		List<Row> rowCollect = row.collectAsList();
//		ArrayList<Node> children = new ArrayList<Node>();
//
//		for(Node node : nodes) {
//			for(Row r : rowCollect) {
//				String id = r.get(0).toString();
//				if(id.equals(node.getValue())) {
//					String[] parts = r.get(1).toString().split("__");
//					String[] associationsArray = parts[1].split(",");
//					node.setName(parts[0]);
//					ArrayList<Node> associationsList = new ArrayList<>();
//					for (String each : associationsArray) {
//						if (each.charAt(each.length() - 1) == ']') {
//							each = each.substring(0, each.length() - 1);
//						}
//						if (each.charAt(0) =='[') {
//							each = each.substring(1);
//						}
//						associationsList.add(new Node(node, each));
//					}
//					node.setChildren(associationsList);
//				}
//			}
//		}
//
//	}
//
//
//	public static ArrayList<Node> getChildren(Node parent) {
//		String whichTable = (parent.getValue().charAt(0) == 'n') ? "crew" : "title";
//		Dataset<Row> row = sparkSession.sql("SELECT assoc FROM global_temp." + whichTable + "_T WHERE id='" + parent.getValue() + "'");
//		List<Row> rowl = row.collectAsList();
//
////		ArrayList<Node> nodes = new ArrayList<>();
////		String searchString = "SELECT id,assoc FROM global_temp.\" + whichTable + \"_T WHERE id='" + nodes.get(0).getValue() + "'";
////		Node head = nodes.remove(0);
////		//Dataset<Row> row = sparkSession.sql("SELECT assoc FROM global_temp." + whichTable + "_T WHERE id='" + parent.getValue() + "'");
////		for(Node n : nodes) {
////			searchString += " OR id='" + n.getValue() + "'";
////		}
////
////		Dataset<Row> row =sparkSession.sql(searchString);
////		List<Row> rowCollect = row.collectAsList();
////		if(rowCollect.size() > 0) return new ArrayList<>();
////		nodes.add(0,head);
////		int index = 0;
////		for(Row r : rowCollect) {
////			String id = r.get(0).toString();
////			String[] parts = r.get(1).toString().split("__")
////			String[] associationsArray = parts[1].split(",");
////			ArrayList<Node> associationsList = new ArrayList<>();
////			for (String each : associationsArray) {
////				if (each.charAt(each.length() - 1) == ']') {
////					each = each.substring(0, each.length() - 1);
////				}
////				associationsList.add(new Node(parent, each));
////			}
////			nodes.get(index).setChildren(associationsList);
////		}
//
//
//
////		System.out.println(row.as([String]));
////		System.out.println("ROW: " + row);
////
////		String[] childs;
////		if(parent.getValue().charAt(0) == 'n') {
////			if(actors.containsKey(parent.getValue())) childs = actors.get(parent.getValue())._2;
////			else return new ArrayList<>();
////		}else {
////			if(titles.containsKey(parent.getValue())) childs = titles.get(parent.getValue())._2;
////			else return new ArrayList<>();
////		}
////		ArrayList<Node> nodes = new ArrayList<>(childs.length);
////		for(String c : childs) {
////			nodes.add(new Node(parent, c));
////		}
////		return nodes;
//
//		if(rowl.size() == 0) return new ArrayList<Node>();
//		String[] parts = rowl.get(0).toString().split("__");
//		String[] associationsArray = parts[1].split(",");
//		parent.setName(parts[0]);
//		ArrayList<Node> associationsList = new ArrayList<>();
//
//		for (String each : associationsArray) {
//			if (each.charAt(each.length() - 1) == ']') {
//				each = each.substring(0, each.length() - 1);
//			}
//			associationsList.add(new Node(parent, each));
//		}
//
//		return associationsList;
//	}
//
//	public static Node bfs() {
//		Node root = new Node(null, sourceID);
//
//		int depth = 0;
//		Queue<Node> nodes = new LinkedList<>();
//		actorsVisited.put(root.getValue(), root.getName());
//		//ArrayList<Node> rootList = new ArrayList<>();
//		root.setChildren(getChildren(root));
//		nodes.offer(root);
//
//		while(!nodes.isEmpty()) {
//			Node node = nodes.poll();
////			for(Node n : node.getChildren()) {
////				System.out.print(n.getValue() + " ");
////			}
////			System.out.println();
//			manyChildren(node.getChildren(), !node.isAnActor());
//			if(node.myDepth() >= 13) break;
//			System.out.println("NODE: " + node.getValue());
//			for(Node n : node.getChildren()) {
//				if (n.getValue().equals(destinationID)) {
//					return n;
//				}
//				if(n.isAnActor() && !actorsVisited.containsKey(n.getValue())) {
//					actorsVisited.put(n.getValue(), n.getName());
//					nodes.add(n);
//				} else if(!n.isAnActor() && !titlesVisited.containsKey(n.getValue())) {
//					titlesVisited.put(n.getValue(), n.getName());
//					nodes.add(n);
//				}
//			}
//		}
//		return null;
//	}
//
//
//	public static void main(String[] args) throws IOException {
//		if(args.length < 2){
//			System.out.println("USAGE: YarnDegreesOfSeparation <nameOfFromPerson> <nameOfToPerson>");
//		}
//		if(args[5].equals("false")) hdfs = "";
//		inputFolder = hdfs+args[0];
//		//titleDataFile = hdfs+args[0];
//		//crewDataFile = hdfs+args[1];
//
//		titlesVisited = Collections.synchronizedMap(new HashMap<String,String>());
//		actorsVisited = Collections.synchronizedMap(new HashMap<String,String>());
//
//		if(hdfs.equals("")) sparkSession =  SparkSession.builder().master("local").appName("Six Degrees").getOrCreate();
//		else sparkSession =  SparkSession.builder().appName("Six Degrees").getOrCreate();
//
//
//		JavaRDD<String> lines = sparkSession.read().textFile(hdfs).toJavaRDD();
//		JavaPairRDD<String, Node> =
//
////		JavaPairRDD<String, String> crewLines = makeRDD(crewDataFile, true);
////		JavaPairRDD<String, String> titleLines = makeRDD(titleDataFile, false);
////
////		crewTable = sparkSession.createDataset(crewLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
////		crewTable.createOrReplaceGlobalTempView("crew_T");
////
////		titleTable = sparkSession.createDataset(titleLines.collect(), Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("id","assoc");
////		titleTable.createOrReplaceGlobalTempView("title_T");
//
////		createHashset(crewDataFile, true, args[3], args[5]);
////		createHashset(titleDataFile, false, "","");
//
//		sourceID = getCrewID(args[1], args[2]);
//		destinationID = getCrewID(args[3], args[4]);
//
//		if (sourceID.isEmpty() || destinationID.isEmpty()) {
//			System.out.println("ERROR: Could not find that person");
//			return;
//		}
//
//		// Run BFS
//		Node path = bfs();
//		String output = "";
//		if(path != null) {
//
//			output = path.toString() + " was found in " + path.getDepth()/2 + " steps!";
//		}else {
//			output = "Cant find path";
//		}
//
//		System.out.println(output);
////        spark.
////        BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));
////        writer.write(output);
////        writer.close();
//
////        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
////
//////Get configuration of Hadoop system
//		if(!hdfs.equals("")) {
//			Configuration hconf = new Configuration();
//			hconf.set("fs.defaultFS", hdfs);
//			System.out.println("Connecting to -- " + hconf.get("fs.defaultFS"));
//
//			hconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			hconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//
//			//System.setProperty("HADOOP_USER_NAME", "hdfs");
//			System.setProperty("hadoop.home.dir", "/");
//			FileSystem fs = FileSystem.get(URI.create(hdfs), hconf);
//
//			Path hdfswritepath = new Path(hdfs + "/" + "results.txt");
//			FSDataOutputStream outputStream = fs.create(hdfswritepath);
//			//Cassical output stream usage
//			outputStream.writeBytes(output);
//			outputStream.close();
//		}
////
//////Destination file in HDFS
////        FileSystem fs = FileSystem.get(URI.create(dst), conf);
////        OutputStream out = fs.create(new Path(dst));
////
//////Copy file from local to HDFS
////        IOUtils.copyBytes(in, out, 4096, true);
//
////        Run DFS
////        ArrayList<String> path = dfs(1, sourceID);
////        System.out.println("Found the path in " + path.size() + " vertices (including movies).");
////        String previous = sourceID;
////
////        for (int i = 0; i < path.size(); ++i) {
////            System.out.println(previous + " was in " + path.get(i) + " with " + path.get(++i));
////            previous = path.get(i);
////        }
//	}
//	//The Long Night Brenda Cowling Glyn Houston Betty McDowall
//	//Sam Safinia Lolita Zackow Bermuda
//}

//mfind "Sam Safinia" "Bermuda" "Lolita Zackow" "Bermuda" true hdfs://raleigh:30101/Movies/output/