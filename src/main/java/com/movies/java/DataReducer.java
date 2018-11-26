package com.movies.java;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;

public class DataReducer {
    private SparkContext spark;

    /**
     * Input files for reducing the dataset
     * Currently includes titles, ratings, title principles and crew
     */
    private String titlesFile;
    private String ratingsFile;
    private String titlePrinciplesFile;
    private String crewFile;

    /**
     * Specify the columns from the input files that will be dropped
     * Each drop array is an array of indices to be dropped
     * Change values however you see fit
     */
    private final int[] titleDrop = {1,3,4,5,6,7,8};  //drop originalTitle, isAdult and endYear
    private final int[] ratingsDrop = {2};   //drop numVotes
    private final int[] titlePrinciplesDrop = {1,3,4,5};   //drop ordering and job
    private final int[] crewDrop = {2,3,4,5};    //drop known for titles

    public static void main(String[] args) {
        DataReducer dr = new DataReducer("src/main/resources/movie.title.basics.test.tsv",
                "src/main/resources/title.ratings.tsv",
                "src/main/resources/title.principles.test.tsv",
                "src/main/resources/crew.names.professions.basics.test.tsv");
        dr.reduceData();
        dr.stop();
    }

    /**
     * DataReducer Constructor, set up spark context and input files
     * @param titlesFile is the input file for movie titles data (From IMDb), this file also includes genre, release year etc.
     * @param ratingsFile is the input file for movie ratings data (From IMDb), this includes title ID and IMDb rating
     * @param titlePrinciplesFile is the input file for titlePrinciples data (From IMDb), this is a file containing
     *        information about which actors, directors, writers etc are in a movie all given by their unique ID
     * @param crewFile is the input file for crew data (From IMDb), this includes name and job category
     */
    public DataReducer(String titlesFile, String ratingsFile, String titlePrinciplesFile, String crewFile) {
        this.spark = new SparkContext(new SparkConf().setMaster("local").setAppName("Test"));
        this.titlesFile = titlesFile;
        this.ratingsFile = ratingsFile;
        this.titlePrinciplesFile = titlePrinciplesFile;
        this.crewFile = crewFile;
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
        JavaRDD<String> lines = spark.textFile(filename,1).toJavaRDD();
        boolean first = true;
        return lines.mapToPair((String s) -> {
            LinkedList list = new LinkedList(Arrays.asList(s.split("\t")));
            String id = list.removeFirst().toString();
            int curr = 0;
            int offset = -1;
            for(int i = 0; i < list.size(); i++) {
                if(curr < drop.length && i == (drop[curr] + offset)) {
                    list.remove(i);
                    offset--;
                    i--;
                    curr++;
                }
            }
            return new Tuple2<>(id, list);
        }).distinct().filter(s -> !s._1.endsWith("t"));
    }

    /**
     * reduceData reduces datasets into smaller and more compact ones
     * specifically reduces data into to files one for movie titles and one for crew
     */
    public void reduceData() {
        JavaPairRDD<String, LinkedList> titles = pairRDDFromFile(titlesFile, titleDrop);
        JavaPairRDD<String, LinkedList> ratings = pairRDDFromFile(ratingsFile, ratingsDrop);
        JavaPairRDD<String, LinkedList> titlePrinciples = pairRDDFromFile(titlePrinciplesFile, titlePrinciplesDrop);
        JavaPairRDD<String, LinkedList> crew = pairRDDFromFile(crewFile, crewDrop);

        reduceTitles(titles, ratings, titlePrinciples);
        reduceCrew(crew, titlePrinciples);
    }

    /**
     * reduceTitles reduces data into single file containing info on movie titles
     * joins RDDs to write to text file each movie, i.e title, genre, release year, cast members
     * @param titles the RDD containing info on titles i.e title, genre, release year
     * @param ratings the RDD containing info on ratings
     * @param titlePrinciples the RDD containing info on the crew in each movie, i.e movie and crew ID's
     */
    public void reduceTitles(JavaPairRDD<String, LinkedList> titles, JavaPairRDD<String, LinkedList> ratings,
                             JavaPairRDD<String, LinkedList> titlePrinciples) {
//        JavaPairRDD<String, LinkedList> titleCombine = titles.mapValues(s ->{
//            LinkedList genres = new LinkedList(Arrays.asList(s.get(5).toString().split(",")));
//            s.set(5, genres);
//            return s;
//        });
        JavaPairRDD<String, Tuple2<String, LinkedList>> joinedTitles = titles.join(titlePrinciples).mapValues(s -> {
            s._1.add(s._2.peekFirst());
            return s._1;
        }).distinct().mapToPair(s ->{
            String title = s._2.remove(0).toString();
            return new Tuple2<>(title, new Tuple2<>(s._1,s._2));
        });

        joinedTitles.coalesce(1).saveAsTextFile("output/joinedTitles.tsv");
    }

    /**
     * reduceCrew reduces data into single file containing info on movie crew
     * joins RDDs to write to text file each crew members info, i.e name movies they've been in, birth year
     * output file form: crewMemberID, [name, birth year, death year,
     * @param crew the RDD containing crew member names, profession and birth year
     * @param titlePrinciples the RDD containing info on the crew in each movie, i.e movie and crew ID's
     */
    public void reduceCrew(JavaPairRDD<String, LinkedList> crew, JavaPairRDD<String, LinkedList> titlePrinciples) {
//        JavaPairRDD<String, LinkedList> crewCombine = crew.mapValues(s ->{
//            LinkedList genres = new LinkedList(Arrays.asList(s.get(4).toString().split(",")));
//            s.set(4,genres);
//            return s;
//        });
        JavaPairRDD<String, LinkedList> crewPrinciples = titlePrinciples.mapToPair(s -> {
            String crewID = s._2.peekFirst().toString();
            s._2.set(0, s._1);
            return new Tuple2<>(crewID,s._2);
        });

        JavaPairRDD<String, Tuple2<String,LinkedList>> joinedCrew = crew.join(crewPrinciples).mapValues(s -> {
            s._1.add(s._2.peekFirst());
            return s._1;
        }).distinct().mapToPair(s ->{
           String name = s._2.remove(0).toString();
           return new Tuple2<>(name, new Tuple2<>(s._1,s._2));
        });
        joinedCrew.coalesce(1).saveAsTextFile("output/joinedCrew.tsv");
    }
}
