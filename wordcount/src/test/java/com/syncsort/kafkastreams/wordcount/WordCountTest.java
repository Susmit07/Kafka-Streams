package com.syncsort.kafkastreams.wordcount;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple WordCountApp.
 * https://kafka.apache.org/20/documentation/streams/developer-guide/testing.html
 * 
 */
public class WordCountTest {

	TopologyTestDriver testDriver;

	StringSerializer stringSerializer = new StringSerializer();

	ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(stringSerializer,
			stringSerializer);

	@Before
	public void setTopologyDriver() {
		Properties kafkaConfigProperties = new Properties();
		kafkaConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
		kafkaConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy.server:9092");
		// if our application is disconnected, when its connected please start from the earliest data available in the topic.
		kafkaConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConfigProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		kafkaConfigProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		testDriver = new TopologyTestDriver(WordCountApp.createBuilderTopology(), kafkaConfigProperties);
	}

	@After
	public void closeTestDriver() {
		testDriver.close();
	}

	@Test
	public void makeSureCountsAreCorrect() {
		String firstExample = "Syncsort changes to Precisely";
		pushNewInputRecord(firstExample);
		OutputVerifier.compareKeyValue(readOutput(), "syncsort", 1L);
		OutputVerifier.compareKeyValue(readOutput(), "changes", 1L);
		OutputVerifier.compareKeyValue(readOutput(), "to", 1L);
		OutputVerifier.compareKeyValue(readOutput(), "precisely", 1L);
		assertEquals(readOutput(), null);

		String secondExample = "Syncsort changes again";
		pushNewInputRecord(secondExample);
		OutputVerifier.compareKeyValue(readOutput(), "syncsort", 2L);
		OutputVerifier.compareKeyValue(readOutput(), "changes", 2L);
		OutputVerifier.compareKeyValue(readOutput(), "again", 1L);

	}

	@Test
	public void makeSureWordsBecomeLowercase() {
		String upperCaseString = "SYNCSORT syncsort syncsort";
		pushNewInputRecord(upperCaseString);
		OutputVerifier.compareKeyValue(readOutput(), "syncsort", 1L);
		OutputVerifier.compareKeyValue(readOutput(), "syncsort", 2L);
		OutputVerifier.compareKeyValue(readOutput(), "syncsort", 3L);

	}

	public ProducerRecord<String, Long> readOutput() {
		return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
	}

	public void pushNewInputRecord(String value) {
		testDriver.pipeInput(recordFactory.create("word-count-input", null, value));
	}

}
