/**
  * @author marinapopova
  * Aug 2, 2019
 */
package org.elasticsearch.kafka.indexer.jobs;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public interface IConsumerWorker extends Runnable {

    /**
     * this method can be overwritten (implemented) in a custom implementation of the ConsumerWorker
     * if you want to expose custom JMX metrics
     * @param previousPollEndPosition
     */
    public void exposeOffsetPositionToJmx(Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition);
    
    /**
     * this method can be overwritten in a custom implementation of the Consumer - 
     * to register this consumer with a JMX exporting service
     */
    public void registerConsumerForJMX();
    
    /**
     * Creates Kafka properties and an instance of a KafkaConsumer
     * 
     * @param consumerInstanceNumber
     */
    public void initConsumerInstance(int consumerInstanceNumber);
    
}