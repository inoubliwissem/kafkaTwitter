package com.kafka;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class Producekafka {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id", "clt");

        kafka.producer.ProducerConfig producerConfig = new kafka.producer.ProducerConfig(properties);
        final Producer<String, String> producer = new Producer<String, String>(producerConfig);

        // parametres Twitter
        String consumerKey = "jGQ9LGcVSUESAPQqSkRPUszJw";
        String consumerSecret = "RmpxEhP6qIEDbts0scOzoLYcbdEA7hjyEwGiqau6wl7xQqTBBQ";
        String accessToken = "2941924528-Wgh9Zklqoh8vgvqmNgXV360qnObwN9OCkZcplzv";
        String accessTokenSecret = "c8PFlGOFgPAzyRx5OqILrrA03wzEVkLjVfRfZQQN5Xynb";
        String[] keyWords = {"bigdata", "java", "hadoop", "mapreduce"};

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);

        /* objet de stream*/
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        FilterQuery fq = new FilterQuery();
        fq.track(keyWords);
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (status.getLang().equals("en")) {
                    KeyedMessage<String, String> message = new <String, String>KeyedMessage("test", status.getText());
                    // KeyedMessage<String,String> msg=new <String, String>KeyedMessage("test",status.getId(),status.getText());
                    producer.send(message);
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onStallWarning(StallWarning sw) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onException(Exception excptn) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        // add listener  
        twitterStream.addListener(listener);
        // add filter
        twitterStream.filter(fq);

    }
}
