package com.sedlarski.eventdriven.twittertokafka.service.listener;

import com.sedlarski.eventdriven.config.KafkaConfigData;
import com.sedlarski.eventdriven.kafka.avro.model.TwitterAvroModel;
import com.sedlarski.eventdriven.twittertokafka.service.transformer.TwitterStatusToAvroTransformer;
import com.sedlarski.kafka.producer.config.service.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;

    @Autowired
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData, KafkaProducer<Long, TwitterAvroModel> kafkaProducer, TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
    }


    @Override
    public void onStatus(Status status) {
        LOGGER.info("Received Twitter status with text: {}", status.getText());
        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
