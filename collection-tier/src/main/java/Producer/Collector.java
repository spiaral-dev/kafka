package Producer;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Collector {

    private static final Logger logger = LoggerFactory.getLogger(Collector.class);
    private final KafkaPublisher publisher;

    Collector(KafkaPublisher publisher) {
        this.publisher = publisher;
    }

    void collect() throws TwitterException, IOException {

        try {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                   .setOAuthConsumerKey("*********************")
                   .setOAuthConsumerSecret("******************************************")
                   .setOAuthAccessToken("**************************************************")
                   .setOAuthAccessTokenSecret("******************************************");

            StatusListener listener = new StatusListener() {

                public void onStatus(Status status) {
                    formatAndPublish(status);
                }

                public void onScrubGeo(long userId, long upToStatusId) {
                }

                public void onStallWarning(StallWarning warning) {
                }

                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                }

                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                }

                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };

            
            FilterQuery query = new FilterQuery();
            String trackParam = "Ratp";
            query.track(trackParam.split(","));
            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
			twitterStream.addListener(listener);
            twitterStream.filter(query);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    void formatAndPublish(Status status){
    	String dateTweet = String.valueOf(status.getCreatedAt());
        String user = status.getUser().getName().replaceAll("[^a-zA-Z0-9]", "");
        String text = new String(status.getText().replaceAll("\n", "").replaceAll("\"", ""));
 	    publisher.publish(keyTweet,"{\"Date\":\""+dateTweet+"\",\"User\":\""+user+"\", \"Text\":\""+text+"\"}");





    }
}
