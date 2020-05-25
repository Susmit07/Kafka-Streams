package com.syncsort.kafkastreams.wordcount;

import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * The class will count the occurrences of words in a Topic.
 * 
 * @author Susmit.Sarkar
 *
 */
public class WordCountApp {

	private static KafkaStreams streams = null;

	private final static Logger log = Logger.getLogger(WordCountApp.class.getName());

	public static void main(String[] args) throws InterruptedException {

		Properties kafkaConfigProperties = new Properties();
		kafkaConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
		kafkaConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.15.100.66:9092");
		// if our application is disconnected, when its connected please start from the
		// earliest data available in the topic.
		kafkaConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConfigProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		kafkaConfigProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		streams = new KafkaStreams(createBuilderTopology(), kafkaConfigProperties);
		// If any exception arises log it.
		streams.setUncaughtExceptionHandler((t, exception) -> {
			log.info(String.format("Stream error on thread: %s", t.getName())+" due to: "+exception.getMessage());
		});
		// delete application local state.
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	public static Topology createBuilderTopology() {
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		// Step1: Get the stream from Kafka topic
		KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input");
		
		KTable<String, Long> wordCounts = wordCountInput.mapValues(text -> text.toLowerCase())
				// Step2: split by space and create individual elements
				.flatMapValues(text -> Arrays.asList(text.split("\\W+")))
				// Step3: replace the key with the value, as the key will be "null"
				.selectKey((key, word) -> word)
				// Step4: group by key before aggregation
				.groupByKey()
				// Step5: count occurrences
				.count(Materialized.as("Word-Counts"));

		// Step6: Publish to Kafka topic
		wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

		return streamsBuilder.build();

	}

	/*
	 * bin/kafka-topics.sh --list --zookeeper localhost:2181 __consumer_offsets
	 * word-count-input word-count-output
	 * wordcount-application-Word-Counts-changelog (Word-Counts is for line 54.
	 * Change-log came as we are doing aggregation on line 52)
	 * wordcount-application-Word-Counts-repartition (Repartition is for line 50, as
	 * we are modifying the key.)
	 */

}
