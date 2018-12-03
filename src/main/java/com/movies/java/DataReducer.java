package com.movies.java;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;

public class DataReducer {
    private SparkSession spark;

    /**
     * Input files for reducing the dataset
     * Currently includes titles, ratings, title principles and crew
     */
    private String titlesFile;
    private String ratingsFile;
    private String titlePrinciplesFile;
    private String crewFile;
    private String output;

    private static String hdfs = "hdfs://raleigh:30101";

    /**
     * Specify the columns from the input files that will be dropped
     * Each drop array is an array of indices to be dropped
     * Change values however you see fit
     */
    private final int[] titleDrop = {0,1,3,4,5,6,7,8};  //drop originalTitle, isAdult and endYear
    private final int[] ratingsDrop = {2};   //drop numVotes
    private final int[] titlePrinciplesDrop = {0,1,3,4,5};   //drop ordering and job
    private final int[] crewDrop = {0,2,3,4,5};    //drop known for titles


    /**
     * args[0] = output folder
     * args[1] = titleBasicsFile
     * args[2] = titlePrinciplesFile
     * args[3] = nameBasicsFile
     * args[4] = isHDFS, if true program runs with hdfs string specified in this file
     */
    public static void main(String[] args) {
        if(args.length > 3) {
            String output = args[0];
            if (args.length > 4 && args[4].equals("false")) hdfs = "";
            String titleBasicInput = args[1];
            String titlePrinciplesInput = args[2];
            String nameBasicInput = args[3];
            //        DataReducer dr = new DataReducer("src/main/resources/movie.title.basics.test.tsv",
            //                "src/main/resources/title.ratings.tsv",
            //                "src/main/resources/title.principles.test.tsv",
            //                "src/main/resources/crew.names.professions.basics.test.tsv",output);
            DataReducer dr = new DataReducer(hdfs + titleBasicInput,
                    hdfs + titlePrinciplesInput,
                    hdfs + nameBasicInput, hdfs + "" + output);
            dr.reduceData();
            dr.stop();
        }
    }

    /**
     * DataReducer Constructor, set up spark context and input files
     * @param titlesFile is the input file for movie titles data (From IMDb), this file also includes genre, release year etc.
     * @param titlePrinciplesFile is the input file for titlePrinciples data (From IMDb), this is a file containing
     *        information about which actors, directors, writers etc are in a movie all given by their unique ID
     * @param crewFile is the input file for crew data (From IMDb), this includes name and job category
     */
    public DataReducer(String titlesFile, String titlePrinciplesFile, String crewFile, String output) {
        //SparkConf conf = new SparkConf().setAppName("Reducer");

        this.spark = SparkSession.builder().appName("Reducer").getOrCreate();
        if(hdfs.equals(""))SparkSession.builder().appName("Reducer").master("local").getOrCreate();
        this.titlesFile = titlesFile;
        this.titlePrinciplesFile = titlePrinciplesFile;
        this.crewFile = crewFile;
        this.output = output;
    }

    public void stop() {
        spark.stop();
    }

    /**
     * pairRDDFromFile creates initial RDDs from given input file dropping unnecessary data
     * @param filename specifies input file
     * @param drop specifies data columns to be dropped from input file
     * @return JavaPairRDD with ID as key and LinkedList of Strings as value
     */
    private JavaPairRDD<String, LinkedList> pairRDDFromFile(String filename, int[] drop) {
        JavaRDD<String> lines = spark.read().textFile(filename).toJavaRDD();
        return lines.mapToPair((String s) -> {
            LinkedList list = new LinkedList(Arrays.asList(s.split("\t")));
            String id = list.peekFirst().toString();
            //int curr = 0;
            int offset = 0;
            for(int i : drop) {
                list.remove(i-offset);
                offset++;
            }

            return new Tuple2<>(id, list);
        });
    }

    /**
     * reduceData reduces datasets into smaller and more compact ones
     * specifically reduces data into to files one for movie titles and one for crew
     */
    public void reduceData() {
        JavaPairRDD<String, LinkedList> titles = pairRDDFromFile(titlesFile, titleDrop);
        JavaPairRDD<String, LinkedList> titlePrinciples = pairRDDFromFile(titlePrinciplesFile, titlePrinciplesDrop);
        JavaPairRDD<String, LinkedList> crew = pairRDDFromFile(crewFile, crewDrop);

        reduceTitles(titles, titlePrinciples);
        //reduceCrew(crew, titlePrinciples);
    }

    /**
     * reduceTitles reduces data into single file containing info on movie titles
     * joins RDDs to write to text file each movie, i.e title, genre, release year, cast members
     * @param titles the RDD containing info on titles i.e title, genre, release year
     * @param titlePrinciples the RDD containing info on the crew in each movie, i.e movie and crew ID's
     */
    public void reduceTitles(JavaPairRDD<String, LinkedList> titles, JavaPairRDD<String, LinkedList> titlePrinciples) {
        JavaPairRDD<String, LinkedList> newTitlePrinciples =titlePrinciples.reduceByKey((x,y) ->{
            x.addAll(y);
            return x;
        });
        JavaPairRDD<String, Tuple2<String, LinkedList>> joinedTitles = titles.join(newTitlePrinciples,10)
                .mapValues(s -> new Tuple2<>(s._1.getFirst().toString(), s._2));

        JavaRDD<String> stringTitles = joinedTitles.map(s -> s._1+"\t"+s._2._1+"\t"+s._2._2);
        stringTitles.coalesce(1,true).saveAsTextFile(output+"/joinedTitles");
    }

    /**
     * reduceCrew reduces data into single file containing info on movie crew
     * joins RDDs to write to text file each crew members info, i.e name movies they've been in, birth year
     * output file form: crewMemberID, [name, birth year, death year,
     * @param crew the RDD containing crew member names, profession and birth year
     * @param titlePrinciples the RDD containing info on the crew in each movie, i.e movie and crew ID's
     */
    public void reduceCrew(JavaPairRDD<String, LinkedList> crew, JavaPairRDD<String, LinkedList> titlePrinciples) {
        JavaPairRDD<String, LinkedList> crewPrinciples = titlePrinciples.mapToPair(s -> {
            String crewID = s._2.peekFirst().toString();
            s._2.set(0, s._1);
            return new Tuple2<>(crewID,s._2);
        }).reduceByKey((x,y) -> {
            x.addAll(y);
            return x;
        });

        JavaPairRDD<String, LinkedList> joinedCrew = crew.join(crewPrinciples,10).mapValues(s -> {
            s._1.addAll(s._2);
            return s._1;
        });

        JavaRDD<String> stringCrew = joinedCrew.map(
                s ->{
                    return s._1+"\t"+s._2.removeFirst()+"\t"+s._2;
                }
        );
        stringCrew.coalesce(1,true).saveAsTextFile(output + "/joinedCrew");
    }
}
