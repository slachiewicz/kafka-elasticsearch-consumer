/**
 * @author marinapopova Apr 13, 2016
 */

package org.elasticsearch.kafka.indexer.jobs;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.kafka.indexer.CommonKafkaUtils;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class ConsumerWorker implements AutoCloseable, IConsumerWorker {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    @Value("${kafka.consumer.source.topic:testTopic}")
    private String kafkaTopic;
    @Value("${application.id:app1}")
    private String consumerInstanceName;
    // interval in MS to poll Kafka brokers for messages, in case there were no messages during the previous interval
    @Value("${kafka.consumer.poll.interval.ms:10000}")
    private long pollIntervalMs;
    @Value("${kafka.consumer.property.prefix:consumer.kafka.property.}")
    private String consumerKafkaPropertyPrefix;
    @Resource(name = "applicationProperties")
    private Properties applicationProperties;

    private OffsetLoggingCallbackImpl offsetLoggingCallback;
    private IBatchMessageProcessor batchMessageProcessor;
    
    private KafkaConsumer<String, String> consumer;
    private AtomicBoolean running = new AtomicBoolean(false);
    private int consumerInstanceId;

    public ConsumerWorker() {        
    }
    
    @Override
    public void initConsumerInstance(int consumerInstanceId) {
        logger.info("init() is starting ....");
        this.consumerInstanceId = consumerInstanceId;
        Properties kafkaProperties = CommonKafkaUtils.extractKafkaProperties(applicationProperties, consumerKafkaPropertyPrefix);
        // add non-configurable properties
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        String consumerClientId = consumerInstanceName + "-" + consumerInstanceId;
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(kafkaProperties);
        registerConsumerForJMX();
        logger.info(
            "Created ConsumerWorker with properties: consumerClientId={}, consumerInstanceName={}, kafkaTopic={}, kafkaProperties={}",
            consumerClientId, consumerInstanceName, kafkaTopic, kafkaProperties);        
    }
    
    @Override
    public void run() {
        running.set(true);
        try {
            logger.info("Starting ConsumerWorker, consumerInstanceId={}", consumerInstanceId);
            consumer.subscribe(Arrays.asList(kafkaTopic), offsetLoggingCallback);
            batchMessageProcessor.onStartup(consumerInstanceId);
            while (running.get()) {
                boolean isPollFirstRecord = true;
                int numProcessedMessages = 0;
                int numFailedMessages = 0;
                int numMessagesInBatch = 0;
                long pollStartMs = 0L;
                logger.debug("consumerInstanceId={}; about to call consumer.poll() ...", consumerInstanceId);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
                batchMessageProcessor.onPollBeginCallBack(consumerInstanceId);
                for (ConsumerRecord<String, String> record : records) {
                    numMessagesInBatch++;
                    logger.debug("consumerInstanceId={}; received record: partition: {}, offset: {}, value: {}",
                            consumerInstanceId, record.partition(), record.offset(), record.value());
                    if (isPollFirstRecord) {
                        isPollFirstRecord = false;
                        logger.info("Start offset for partition {} in this poll : {}", record.partition(), record.offset());
                        pollStartMs = System.currentTimeMillis();
                    }
                    try {
                        boolean processedOK = batchMessageProcessor.processMessage(record, consumerInstanceId);
                        if (processedOK) {
                            numProcessedMessages++;
                        } else {
                            FailedEventsLogger.logFailedEvent("Failed to process event: ", 
                                record.value(), record.offset());
                            numFailedMessages++;                            
                        }
                    } catch (Exception e) {
                        FailedEventsLogger.logFailedEventWithException(e.getMessage(), record.value(), record.offset(), e);
                        numFailedMessages++;
                    }
                }
                long endOfPollLoopMs = System.currentTimeMillis();
                batchMessageProcessor.onPollEndCallback(consumerInstanceId);
                if (numMessagesInBatch > 0) {
                    Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition = getPreviousPollEndPosition();
                    boolean shouldCommitThisPoll = batchMessageProcessor.beforeCommitCallBack(consumerInstanceId, previousPollEndPosition);
                    long afterProcessorCallbacksMs = System.currentTimeMillis();
                    commitOffsetsIfNeeded(shouldCommitThisPoll, previousPollEndPosition);
                    long afterOffsetsCommitMs = System.currentTimeMillis();
                    exposeOffsetPosition(previousPollEndPosition);
                    logger.info(
                        "Last poll snapshot: numMessagesInBatch: {}, numProcessedMessages: {}, numFailedMessages: {}, " + 
                        "timeToProcessLoop: {}ms, timeInMessageProcessor: {}ms, timeToCommit: {}ms, totalPollTime: {}ms",
                        numMessagesInBatch, numProcessedMessages, numFailedMessages,
                        endOfPollLoopMs - pollStartMs,
                        afterProcessorCallbacksMs - endOfPollLoopMs,
                        afterOffsetsCommitMs - afterProcessorCallbacksMs,
                        afterOffsetsCommitMs - pollStartMs);
                } else {
                    logger.info("No messages recieved during this poll");
                }
            }
        } catch (WakeupException e) {
            logger.warn("ConsumerWorker [consumerInstanceId={}] got WakeupException - exiting ...", consumerInstanceId, e);
            // ignore for shutdown
        } catch (Throwable e) {
            logger.error("ConsumerWorker [consumerInstanceId={}] got Throwable Exception - will exit ...", consumerInstanceId, e);
            throw new RuntimeException(e);
        } finally {
            logger.warn("ConsumerWorker [consumerInstanceId={}] is shutting down ...", consumerInstanceId);
            offsetLoggingCallback.getPartitionOffsetMap()
                .forEach((topicPartition, offset)
                     -> logger.info("Offset position during the shutdown for consumerInstanceId : {}, partition : {}, offset : {}",
                     consumerInstanceId, topicPartition.partition(), offset.offset()));
            batchMessageProcessor.onShutdown(consumerInstanceId);
            consumer.close();
        }
    }
 
    /**
     * this method can be overwritten (implemented) in your own ConsumerManager 
     * if you want to expose custom JMX metrics
     * @param previousPollEndPosition
     */
    public void exposeOffsetPosition(Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition) {
        // NO OP
    }
    
    public void registerConsumerForJMX() {
        // NO OP
    }
   
    private void commitOffsetsIfNeeded(boolean shouldCommitThisPoll, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap) {
        try {
            if (shouldCommitThisPoll) {
                long commitStartTime = System.nanoTime();
                if(partitionOffsetMap == null){
                    consumer.commitAsync(offsetLoggingCallback);
                } else {
                    consumer.commitAsync(partitionOffsetMap, offsetLoggingCallback);
                }
                long commitTime = System.nanoTime() - commitStartTime;
                logger.info("Commit successful for partitions/offsets : {} in {} ns", partitionOffsetMap, commitTime);
            } else {
                logger.debug("shouldCommitThisPoll = FALSE --> not committing offsets yet");
            }
        } catch (RetriableCommitFailedException e){
            logger.error("caught RetriableCommitFailedException while committing offsets : {}; abandoning the commit", partitionOffsetMap, e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getPreviousPollEndPosition() {
        Map<TopicPartition, OffsetAndMetadata> nextCommitableOffset = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            nextCommitableOffset.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)));
        }
        return nextCommitableOffset;
    }

    @Override
    public void close() {
        logger.warn("ConsumerWorker [consumerInstanceId={}] shutdown() is called  - will call consumer.wakeup()", consumerInstanceId);
        running.set(false);
        consumer.wakeup();
    }

    public void shutdown() {
        logger.warn("ConsumerWorker [consumerInstanceId={}] shutdown() is called  - will call consumer.wakeup()", consumerInstanceId);
        running.set(false);
        consumer.wakeup();
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return offsetLoggingCallback.getPartitionOffsetMap();
    }

    public int getConsumerInstanceId() {
        return consumerInstanceId;
    }

    public void setBatchMessageProcessor(IBatchMessageProcessor batchMessageProcessor) {
        this.batchMessageProcessor = batchMessageProcessor;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public void setOffsetLoggingCallback(OffsetLoggingCallbackImpl offsetLoggingCallback) {
        this.offsetLoggingCallback = offsetLoggingCallback;
    }

}
