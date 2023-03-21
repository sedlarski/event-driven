package com.sedlarski.eventdriven.kafka.admin.client;

import com.sedlarski.eventdriven.config.KafkaConfigData;
import com.sedlarski.eventdriven.config.RetryConfigData;
import com.sedlarski.eventdriven.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData,
                            RetryConfigData retryConfigData,
                            AdminClient adminClient,
                            RetryTemplate retryTemplate,
                            WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(retryContext -> this.doCreateTopics(retryContext));
        } catch (Throwable t) {
            throw new RuntimeException("Reached max number of retry for creating kafka topic(s)", t);
        }
        checkTopicsCreated();
    }

    private void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Long maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTime = retryConfigData.getSleepTimeMs();
        for(String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while(!isTopicsCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTime);
                sleepTime *= multiplier;
                topics = getTopics();
            }
        }
    }

    private void sleep(Long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Interrupted while sleeping", e);
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient.get()
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .retrieve()
                    .toEntity(String.class)
                    .block()
                    .getStatusCode();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Long maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTime = retryConfigData.getSleepTimeMs();
        while(getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTime);
            sleepTime *= multiplier;
        }
    }

    private void checkMaxRetry(int retry, Long maxRetry) {
        if(retry > maxRetry) {
            throw new KafkaClientException("Reached max number of retry for checking if topic was created");
        }
    }

    private boolean isTopicsCreated(Collection<TopicListing> topics, String topicName) {
        if(topics == null) {
            return false;
        }
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Creating {}  topics, attempt {}", topicNames.size(), retryContext.getRetryCount());
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());
        return adminClient.createTopics(kafkaTopics);
    }
    
    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for getting kafka topics", t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        LOG.info("Reading tiopic {}, attempt {}", kafkaConfigData.getTopicName(), retryContext.getRetryCount());
        Collection<TopicListing>  topics = adminClient.listTopics().listings().get();
        if(topics != null) {
            topics.forEach(topic -> LOG.debug("Topic: {}", topic.name()));
        }
        return topics;
    }


}
