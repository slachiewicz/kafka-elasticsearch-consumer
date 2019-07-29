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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWorker implements Runnable, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private IBatchMessageProcessor batchMessageProcessor;
    private final KafkaConsumer<String, String> consumer;
    private final String kafkaTopic;
    private final int consumerId;
    // interval in MS to poll Kafka brokers for messages, in case there were no
    // messages during the previous interval
    private long pollIntervalMs;
    private OffsetLoggingCallbackImpl offsetLoggingCallback;
    private AtomicBoolean running = new AtomicBoolean(false);
    private String consumerGroupName;

    public ConsumerWorker(
            int consumerId, 
            String consumerInstanceName, 
            String kafkaTopic, 
            Properties kafkaProperties,
            long pollIntervalMs, 
            IBatchMessageProcessor batchMessageProcessor, 
            OffsetLoggingCallbackImpl offsetLoggingCallback) {
        this.batchMessageProcessor = batchMessageProcessor;
        this.offsetLoggingCallback = offsetLoggingCallback;
        this.consumerId = consumerId;
        this.kafkaTopic = kafkaTopic;
        this.pollIntervalMs = pollIntervalMs;
        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerInstanceName + "-" + consumerId);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(kafkaProperties);
        if (kafkaProperties != null && kafkaProperties.get(ConsumerConfig.GROUP_ID_CONFIG) != null) {
            this.consumerGroupName = (String) kafkaProperties.get(ConsumerConfig.GROUP_ID_CONFIG);
        }
        this.consumerGroupName = StringUtils.defaultString(consumerGroupName, StringUtils.SPACE);
        logger.info(
            "Created ConsumerWorker with properties: consumerId={}, consumerInstanceName={}, kafkaTopic={}, kafkaProperties={}",
            consumerId, consumerInstanceName, kafkaTopic, kafkaProperties);
    }

    @Override
    public void run() {
        running.set(true);
        try {
            logger.info("Starting ConsumerWorker, consumerId={}", consumerId);
            consumer.subscribe(Arrays.asList(kafkaTopic), offsetLoggingCallback);
            batchMessageProcessor.onStartup(consumerId);
            while (running.get()) {
                boolean isPollFirstRecord = true;
                int numProcessedMessages = 0;
                int numFailedMessages = 0;
                int numMessagesInBatch = 0;
                long pollStartMs = 0L;
                logger.debug("consumerId={}; about to call consumer.poll() ...", consumerId);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
                batchMessageProcessor.onPollBeginCallBack(consumerId);
                for (ConsumerRecord<String, String> record : records) {
                    numMessagesInBatch++;
                    logger.debug("consumerId={}; received record: partition: {}, offset: {}, value: {}",
                            consumerId, record.partition(), record.offset(), record.value());
                    if (isPollFirstRecord) {
                        isPollFirstRecord = false;
                        logger.info("Start offset for partition {} in this poll : {}", record.partition(), record.offset());
                        pollStartMs = System.currentTimeMillis();
                    }
                    try {
                        boolean processedOK = batchMessageProcessor.processMessage(record, consumerId);
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
                batchMessageProcessor.onPollEndCallback(consumerId);
                if (numMessagesInBatch > 0) {
                    Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition = getPreviousPollEndPosition();
                    boolean shouldCommitThisPoll = batchMessageProcessor.beforeCommitCallBack(consumerId, previousPollEndPosition);
                    long afterProcessorCallbacksMs = System.currentTimeMillis();
                    commitOffsetsIfNeeded(shouldCommitThisPoll, previousPollEndPosition);
                    long afterOffsetsCommitMs = System.currentTimeMillis();
                    exposeOffsetPositionToJmx(previousPollEndPosition);
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
            logger.warn("ConsumerWorker [consumerId={}] got WakeupException - exiting ...", consumerId, e);
            // ignore for shutdown
        } catch (Throwable e) {
            logger.error("ConsumerWorker [consumerId={}] got Throwable Exception - will exit ...", consumerId, e);
            throw new RuntimeException(e);
        } finally {
            logger.warn("ConsumerWorker [consumerId={}] is shutting down ...", consumerId);
            offsetLoggingCallback.getPartitionOffsetMap()
                .forEach((topicPartition, offset)
                     -> logger.info("Offset position during the shutdown for consumerId : {}, partition : {}, offset : {}",
                     consumerId, topicPartition.partition(), offset.offset()));
            batchMessageProcessor.onShutdown(consumerId);
            consumer.close();
        }
    }
 
    private void exposeOffsetPositionToJmx(Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition) {
        // NO OP
        // this method can be overwritten (implemented) in your own ConsumerManager 
        // if you want to expose custom JMX metrics
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
        logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
        running.set(false);
        consumer.wakeup();
    }

    public void shutdown() {
        logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
        running.set(false);
        consumer.wakeup();
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return offsetLoggingCallback.getPartitionOffsetMap();
    }

    public int getConsumerId() {
        return consumerId;
    }
}
