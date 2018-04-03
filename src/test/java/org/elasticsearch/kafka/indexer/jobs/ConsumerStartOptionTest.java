/**
  * @author marinapopova
  * Apr 2, 2018
 */
package org.elasticsearch.kafka.indexer.jobs;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.kafka.indexer.jobs.ConsumerStartOption.StartFrom;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author marinapopova
 *
 */
public class ConsumerStartOptionTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * Test method for {@link org.elasticsearch.kafka.indexer.jobs.ConsumerStartOption#fromFile(java.lang.String)}.
	 */
	@Test
	public void testFromFile_restart() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromFile(
			"src/test/resources/test-start-options-restart.config");
		Assert.assertNotNull(configMap);
		Assert.assertTrue(configMap.isEmpty());
	}

	@Test
	public void testFromFile_empty() {
		Map<Integer, ConsumerStartOption> configMap = ConsumerStartOption.fromFile("");
		Assert.assertNotNull(configMap);
		Assert.assertTrue(configMap.isEmpty());
	}

	@Test
	public void testFromFile_earliest() {
		Map<Integer, ConsumerStartOption> expectedMap = new HashMap<>();
		expectedMap.put(ConsumerStartOption.DEFAULT, 
			new ConsumerStartOption(ConsumerStartOption.DEFAULT, StartFrom.EARLIEST, 0L));
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromFile(
			"src/test/resources/test-start-options-earliest.config");
		Assert.assertNotNull(resultMap);
		Assert.assertTrue(expectedMap.equals(resultMap));
	}

	@Test
	public void testFromFile_custom() {
		Map<Integer, ConsumerStartOption> expectedMap = new HashMap<>();
		expectedMap.put(ConsumerStartOption.DEFAULT, 
			new ConsumerStartOption(ConsumerStartOption.DEFAULT, StartFrom.EARLIEST, 0l));
		expectedMap.put(0, new ConsumerStartOption(0, StartFrom.CUSTOM, 10L));
		expectedMap.put(1, new ConsumerStartOption(1, StartFrom.CUSTOM, 20L));
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromFile(
			"src/test/resources/test-start-options-custom.config");
		Assert.assertNotNull(resultMap);
		Assert.assertTrue(expectedMap.equals(resultMap));
	}

	@Test
	public void testFromFile_custom_with_restart() {
		// if there are mixed options - RESTARt for some partitions, but not RESTART for default option - 
		// still should be treated as a RESTART, and, thus, the map shoudl be empty
		Map<Integer, ConsumerStartOption> resultMap = ConsumerStartOption.fromFile(
			"src/test/resources/test-start-options-custom-with-restart.config");
		Assert.assertNotNull(resultMap);
		Assert.assertTrue(resultMap.isEmpty());
	}

}
