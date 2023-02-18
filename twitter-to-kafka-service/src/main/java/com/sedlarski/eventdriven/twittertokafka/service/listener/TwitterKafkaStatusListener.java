package com.sedlarski.eventdriven.twittertokafka.service.listener;

import com.sedlarski.eventdriven.twittertokafka.service.TwitterToKafkaApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    @Override
    public void onStatus(Status status) {
        LOGGER.info("Received Twitter status with text: {}", status.getText());
    }
}
