package com.sedlarski.eventdriven.twittertokafka.service;

import com.sedlarski.eventdriven.twittertokafka.service.config.TwitterToKafkaServiceConfigData;
import com.sedlarski.eventdriven.twittertokafka.service.runner.impl.TwitterKafkaStreamRunner;
import com.sedlarski.eventdriven.twittertokafka.service.runner.impl.TwitterStreamV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwitterToKafkaApplication implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterToKafkaApplication.class);

    private final TwitterKafkaStreamRunner twitterKafkaStreamRunner;

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterStreamV2 twitterStreamV2;

    public TwitterToKafkaApplication(TwitterKafkaStreamRunner twitterKafkaStreamRunner, TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterStreamV2 twitterStreamV2) {
        this.twitterKafkaStreamRunner = twitterKafkaStreamRunner;
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterStreamV2 = twitterStreamV2;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("App starts...");
        LOGGER.info("Twitter keywords: {}", twitterToKafkaServiceConfigData.getTwitterKeywords());
        LOGGER.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
//        twitterKafkaStreamRunner.start();
        twitterStreamV2.start(twitterToKafkaServiceConfigData.getBearerToken());


    }
}
