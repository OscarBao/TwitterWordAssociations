import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.tools.internal.jxc.ap.Const;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by Oscar_Local on 8/27/2016.
 */
public class TwitterWordAssociations {

    static Gson gson;
    static String[] filters;

    public static void main(String[] args) {

        //Pre-spark setup
        gson = new GsonBuilder().create();

        //Setting up a Spark configuration
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("test_spark_2_streaming");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");

        //Setting up a Twitter Streaming config
        System.setProperty("twitter4j.jsonStoreEnabled", "true");
        System.setProperty("twitter4j.oauth.consumerKey", Constants.CONSUMER_KEY);
        System.setProperty("twitter4j.oauth.consumerSecret", Constants.CONSUMER_SECRET);
        System.setProperty("twitter4j.oauth.accessToken", Constants.ACCESS_TOKEN);
        System.setProperty("twitter4j.oauth.accessTokenSecret", Constants.ACCESS_TOKEN_SECRET);
        Duration batchInterval = new Duration(Constants.BATCH_RETRIEVE_DELAY);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, batchInterval);
        Configuration twitterConf = ConfigurationContext.getInstance();


        //Before doing anything, make sure file is deleted


        //Write to a file
            File tweetFile = new File(Constants.TWEET_FILE_NAME);
            tweetFile.delete();

            filters = args;

            //write stream
            JavaDStream<String> batchTweetJsonsStream = getTweetsJsonStream(jssc, twitterConf, filters);
            batchTweetJsonsStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                    for (String str : stringJavaRDD.collect()) {
                        BufferedWriter bw = new BufferedWriter(new FileWriter(Constants.TWEET_FILE_NAME, true));
                        writeToFile(bw, str);
                        bw.close();
                    }
                }
            });




            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new ProcessMessagesProcess(jsc, Constants.TWEET_FILE_NAME), Constants.BATCH_RETRIEVE_DELAY+1000, Constants.UPDATE_RESULTS_DELAY);

            jssc.start();
            jssc.awaitTermination();








    }

    private static class ProcessMessagesProcess extends TimerTask {
        JavaSparkContext jsc;
        String fileToProcess;

        public ProcessMessagesProcess(JavaSparkContext jsc, String fileToProcess) {
            this.jsc = jsc;
            this.fileToProcess = fileToProcess;
        }

        @Override
        public void run() {
            System.out.println("============RETRIEVING RESULTS============");
            System.out.println("..........................................");
            List<Tuple2<String, Integer>> topMentionedWordsWithCount = getTopMentionedWords(jsc, fileToProcess);
            if(topMentionedWordsWithCount.size() <= 0 ||
                    topMentionedWordsWithCount.get(0)._2 < Constants.QUALIFIED_COUNTS_SIZE) {
                System.out.println("Not enough tweets yet...");
                return;
            }
            List<String> topMentionedWords = new ArrayList<>();
            List<Integer> topMentionedCounts = new ArrayList<>();
            for(Tuple2<String, Integer> tuple : topMentionedWordsWithCount) {
                topMentionedWords.add(tuple._1);
                topMentionedCounts.add(tuple._2);
            }
            System.out.println("=============PRINTING RESULTS=============");
            System.out.println("\t\tTop words tweeted alongside " + Arrays.asList(filters));
            System.out.format("\t%10s |\t%5s\n", "Word", "Count");
            System.out.println("\t-----------------------");
            for(int i = 0; i < topMentionedWords.size(); i++) {
                System.out.format("\t%10s |\t%5d\n", topMentionedWords.get(i), topMentionedCounts.get(i));
            }
            System.out.println("==========================================\n\n");
        }
    }


    public static List<Tuple2<String, Integer>> getTopMentionedWords(JavaSparkContext jsc, String nameOfFile) {
        JavaRDD<String> collectedMessagesRDD = readCollectedMessages(jsc, nameOfFile);
        JavaPairRDD<String, Integer> mentionedWordsCounts =
            collectedMessagesRDD.flatMap(s -> Arrays.asList(s.split("[\\p{Punct}\\s]+")))
                    .filter(new Function<String, Boolean>() {
                        //Filter out short, common, and uncomprehensible words
                        @Override
                        public Boolean call(String s) throws Exception {
                            if(s.length() <= Constants.DISQUALIFICATION_LENGTH) return false;
                            if(s.contains("?")) return false;
                            for(int i = 0; i < Constants.DISQUALIFIED_WORDS.length; i++) {
                                if(s.toLowerCase().contains(Constants.DISQUALIFIED_WORDS[i])) return false;
                            }
                            for(int i = 0; i < filters.length; i++) {
                                if(s.toLowerCase().contains(filters[i])) return false;
                            }
                            return true;
                        }
                    })
                    .mapToPair(new PairFunction<String, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(String s) throws Exception {
                            return new Tuple2<>(s, 1);
                        }
                    }).reduceByKey((i1, i2) -> i1 + i2).mapToPair(Tuple2::swap).sortByKey(false)
                            .mapToPair(Tuple2::swap);

        return mentionedWordsCounts.take(Constants.NUMBER_TOPS_TO_SHOW);
    }

    public static JavaRDD<String> readCollectedMessages(JavaSparkContext jsc, String nameOfFile) {
        JavaRDD<String> messageJsons = jsc.textFile(nameOfFile);
        JavaRDD<String> collectiveReadableMessages = messageJsons.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                TweetMessage tm = gson.fromJson(s, TweetMessage.class);
                return tm.getText();
            }
        });

        return collectiveReadableMessages;
    }


    public static void writeToFile(BufferedWriter writer, String message) {
        try {
            writer.write(message);
            writer.newLine();
            writer.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JavaDStream<String> getTweetsJsonStream(JavaStreamingContext jssc, Configuration twitterConfig, String[] filters) {
        JavaDStream<String> tweetsJsonStream = TwitterUtils.createStream(jssc, AuthorizationFactory.getInstance(twitterConfig), filters)
                .map(new Function<Status, String>() {
                    @Override
                    public String call(Status status) throws Exception {
                        TweetMessage tm = new TweetMessage(status.getText(), status.getUser().getName());
                        return gson.toJson(tm);
                    }
                });
        return tweetsJsonStream;

    }

}