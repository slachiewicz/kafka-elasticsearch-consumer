/**
  * @author marinapopova
  * Jul 17, 2019
 */
package org.elasticsearch.kafka.indexer;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonKafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonKafkaUtils.class);

    public static void extractKafkaProperties(Properties applicationProperties, Properties kafkaProperties, String kafkaPropertyPrefix) {
        if(applicationProperties == null || applicationProperties.size() <= 0){
            LOGGER.info("No application kafka properties are found to set");
            return;
        }
        for(Map.Entry <Object,Object> currentPropertyEntry :applicationProperties.entrySet()){
            String propertyName = currentPropertyEntry.getKey().toString();
            if(StringUtils.isNotBlank(propertyName) && propertyName.contains(kafkaPropertyPrefix)){
                String validKafkaConsumerProperty = propertyName.replace(kafkaPropertyPrefix, StringUtils.EMPTY);
                kafkaProperties.put(validKafkaConsumerProperty, currentPropertyEntry.getValue());
                LOGGER.info("Adding kafka property with prefix: {}, key: {}, value: {}", kafkaPropertyPrefix, validKafkaConsumerProperty, currentPropertyEntry.getValue());
            }
        }
    }

}
