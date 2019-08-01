package org.elasticsearch.kafka.indexer;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CommonKafkaUtilsTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testExtractKafkaProperties() {
        String testPrefix = "prefix.";
        String kafkaProperty1 = "kafkaProperty1";
        String kafkaProperty2 = "kafkaProperty1";
        String kafkaPropertyValue1 = "value1";
        String kafkaPropertyValue2 = "value2";
        Properties inputApplicationProperties = new Properties();
        
        // add two qualifying Kafka properties to the map and one non-qualifying (not a Kafka property)
        inputApplicationProperties.put(testPrefix + kafkaProperty1, kafkaPropertyValue1);
        inputApplicationProperties.put(testPrefix + kafkaProperty2, kafkaPropertyValue2);
        inputApplicationProperties.put("notKafkaProperty", "unrelatedValue");

        Properties expectedKafkaProperties = new Properties();
        expectedKafkaProperties.put(kafkaProperty1, kafkaPropertyValue1);
        expectedKafkaProperties.put(kafkaProperty2, kafkaPropertyValue2);

        Properties resultKafkaProperties = CommonKafkaUtils.extractKafkaProperties(inputApplicationProperties, testPrefix);
        Assert.assertEquals(expectedKafkaProperties, resultKafkaProperties);
    }

}
