/**
 * Created by Oscar_Local on 8/27/2016.
 */
public class TweetMessage {
    String text;
    String user;

    public TweetMessage(String text, String user) {
        this.text = text;
        this.user = user;
    }

    public String getText() {
        return text;
    }

    public String getUser() {
        return user;
    }
}
