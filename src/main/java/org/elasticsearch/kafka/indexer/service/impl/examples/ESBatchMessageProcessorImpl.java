/**
  * @author marinapopova
  * Feb 24, 2016
 */
package org.elasticsearch.kafka.indexer.service.impl.examples;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.elasticsearch.kafka.indexer.exception.ConsumerRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

/**
 * 
 * This is an example of a Batch Message Processor that pushes (indexes) events 
 * collected in one poll() to ElasticSearch;
 * You can write your own batch processor that pushes events into some other destination - 
 * for example Cassandra, Postgres, Spark, etc.
 *
 */
public class ESBatchMessageProcessorImpl implements IBatchMessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ESBatchMessageProcessorImpl.class);

    private ElasticSearchBatchService elasticSearchBatchService = null;
	@Value("${elasticsearch.index.name:my_index}")
	private String indexName;
	@Value("${elasticsearch.index.type:varnish}")
	private String indexType;

    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#processMessage(org.apache.kafka.clients.consumer.ConsumerRecord, int)
     */
    @Override
    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord, int consumerId) throws Exception {
        String eventUUID = null; // we don't need a UUID for this simple scenario
        String routingValue = null; // we don't need routing for this simple scenario       
        String inputMessage = currentKafkaRecord.value();
        if (StringUtils.isEmpty(inputMessage)) {
            return false;
        }
        elasticSearchBatchService.addEventToBulkRequest(
                inputMessage, indexName, indexType, eventUUID, routingValue);
        return true;
    }
    
    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#onPollBeginCallBack(int)
     */
    @Override
    public void onPollBeginCallBack(int consumerId) throws Exception {
        // NO OP        
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#beforeCommitCallBack(int, java.util.Map)
     */
    @Override
    public boolean onPollEndCallBack(int consumerId, Map<TopicPartition, OffsetAndMetadata> previousPollEndPosition)
            throws Exception {
        boolean commitOffset = true;
        try {
            elasticSearchBatchService.postToElasticSearch();
        } catch (IndexerESRecoverableException e) {
            // if this is a re-coverable exception - do NOT commit the offsets, let events 
            // from this poll be re-processed
            commitOffset = false;
            logger.error("Recoverable Error posting messages to Elastic Search: {}", e.getMessage());
            throw new ConsumerRecoverableException("Error posting messages to Elastic Search", e);
        }
        return commitOffset;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#onStartup(int)
     */
    @Override
    public void onStartup(int consumerId) throws Exception {
        // NO OP        
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.kafka.indexer.service.IBatchMessageProcessor#onShutdown(int)
     */
    @Override
    public void onShutdown(int consumerId) {
        // NO OP        
    }

    public ElasticSearchBatchService getElasticSearchBatchService() {
        return elasticSearchBatchService;
    }

    public void setElasticSearchBatchService(ElasticSearchBatchService elasticSearchBatchService) {
        this.elasticSearchBatchService = elasticSearchBatchService;
    }
}
