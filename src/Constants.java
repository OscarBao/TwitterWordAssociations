/**
 * Created by Oscar_Local on 8/27/2016.
 */
public interface Constants {
    String[] DISQUALIFIED_WORDS = {"RT", "t", "https", "co", "your", "like", "really", "would", "have", "some", "very", "more", "doing",
                                    "going", "always", "having", "been", "already"};
    int DISQUALIFICATION_LENGTH = 4;
    int QUALIFIED_COUNTS_SIZE = 20;
    int NUMBER_TOPS_TO_SHOW = 20;

    String TWEET_FILE_NAME = "tweets.txt";

    int BATCH_RETRIEVE_DELAY = 10*1000;
    int UPDATE_RESULTS_DELAY = 60*1000;


    //Auth credentials
    String CONSUMER_KEY = "cEKscKOWtgjRtVB0B4eEyLwvs";
    String CONSUMER_SECRET = "87170YiULJchRRIPvky2yzAyGv0ksGKwH0g7wfo5eaEYh0jUGv";
    String ACCESS_TOKEN = "768755121391931393-qgna25mm5Ytb1CEmf2iYnlfvBviCX1Y";
    String ACCESS_TOKEN_SECRET = "Hp8wIY7iRBSX9grucbUDBIodWB6ALtoopaafe0vmk4Js8";
}
