/**
  * @author marinapopova
  * May 2, 2016
 */
package org.elasticsearch.kafka.indexer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface IBatchMessageProcessor {

	/**
	 * Process one message from Kafka - do parsing or any other logic required
	 * returns a boolean to if message is processed successfully or not
	 * 
	 *
	 * @param currentKafkaRecord
	 * @param consumerId ID of the consumer thread processing this message
	 * @return isBatchCompleted
	 * @throws Exception
	 */
    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord, int consumerId) throws Exception;
    
    /**
     * callback method - called after each poll() request to Kafka brokers is done
     * but before any messages from the retrieved batch start to get processed
     * 
     * @param consumerId
     * @throws Exception
     */
    public void onPollBeginCallBack(int consumerId) throws Exception;

	/**
	 * callback method - called after all events from the last poll() were processed;
	 * returns a flag indicating whether offsets for this poll() should be committed or not;
	 * This allows custom MessageProcessor implementations to control how many polls per bath
	 * they want to process;
	 * Default is: 1 poll == 1 batch ==> offsets are committed (on successful processing)
	 *
	 * Returning TRUE form this method means that the offsets form the last poll() 
	 * (and any other previously un-committed polls) will be committed;
	 * Returning FALSE means that the offsets will NOT be committed and events from the previous poll will
	 * be re-processed
	 * 
	 * @param consumerId
	 * @return isBatchCompleted: if true, offsets should be committed ; if FALSE - not
	 * @throws Exception
	 */
	public boolean onPollEndCallback(int consumerId) throws Exception;


	public boolean beforeCommitCallBack(int consumerId, Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition) throws Exception;

	public void onStartup(int consumerId) throws Exception;

	public void onShutdown(int consumerId) ;

}
