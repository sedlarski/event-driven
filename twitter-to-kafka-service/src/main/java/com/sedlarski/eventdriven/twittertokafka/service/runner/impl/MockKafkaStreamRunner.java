package com.sedlarski.eventdriven.twittertokafka.service.runner.impl;

import com.sedlarski.eventdriven.config.TwitterToKafkaServiceConfigData;
import com.sedlarski.eventdriven.twittertokafka.service.exception.TwitterToKafkaServiceException;
import com.sedlarski.eventdriven.twittertokafka.service.listener.TwitterKafkaStatusListener;
import com.sedlarski.eventdriven.twittertokafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[] {
        "Lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua", "Ut", "enim", "ad", "minim", "veniam", "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea", "commodo", "consequat", "Duis", "aute", "irure", "dolor", "in", "reprehenderit", "in", "voluptate", "velit", "esse", "cillum", "dolore", "eu", "fugiat", "nulla", "pariatur", "Excepteur", "sint", "occaecat", "cupidatat", "non", "proident", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit", "anim", "id", "est", "laborum"
    };

    private static final String tweetAsRawJson = "{\n" +
        "  \"created_at\": \"{0}\",\n" +
        "  \"id\": {1},\n" +
        "  \"text\": \"{2}\",\n" +
        "  \"user\": {\n" +
        "    \"id\": {3}\n }" +
            "}";

    private static final String TWITTER_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";



    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }


    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTime = twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mock stream runner with keywords: {}, minTweetLength: {}, maxTweetLength: {}, sleepTime: {}", keywords, minTweetLength, maxTweetLength, sleepTime);
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTime);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTime) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while(true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTime);
                }
            } catch (TwitterException e) {
                LOG.error("Error while creating status from tweet", e);
            }
        });

    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String [] params = new String[] {
            getFormattedDate(),
            String.valueOf(getRandomId()),
            getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
            String.valueOf(getRandomId())
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }

    private Long getRandomId() {
        return ThreadLocalRandom.current().nextLong();
    }

    private String getFormattedDate() {
        return ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_DATE_FORMAT, Locale.ENGLISH));
    }
}
