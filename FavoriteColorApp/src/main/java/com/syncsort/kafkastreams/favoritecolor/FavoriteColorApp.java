package com.syncsort.kafkastreams.favoritecolor;

import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Favorite color occurrences for each user.
 * 
 * @author Susmit.Sarkar
 *
 */
public class FavoriteColorApp {

	private static KafkaStreams streams = null;

	private final static Logger log = Logger.getLogger(FavoriteColorApp.class.getName());

	public static void main(String[] args) throws InterruptedException {

		Properties kafkaConfigProperties = new Properties();
		kafkaConfigProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
		kafkaConfigProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.15.100.66:9092");
		// if our application is disconnected, when its connected please start from the earliest data available in the topic.
		kafkaConfigProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		kafkaConfigProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		kafkaConfigProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		streams = new KafkaStreams(createBuilderTopology(), kafkaConfigProperties);
		// If any exception arises log it.
		streams.setUncaughtExceptionHandler((t, exception) -> {
			log.info(String.format("Stream error on thread: %s", t.getName()) + " due to: " + exception.getMessage());
		});
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	private static Topology createBuilderTopology() {
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		// Step1: Get the stream from Kafka topic
		// It will contain all the users choice of colors
		KStream<String, String> favoriteColorInput = streamsBuilder.stream("favourite-colour-input");

		KStream<String, String> usersAndColours = favoriteColorInput
				// Step1: Ensure that a comma is here as we will split on it
				.filter((key, value) -> value.contains(","))
				// Step2: Split by comma and take the first element (userId) and convert to lower case for uniformity and safety.
				.selectKey((key, value) -> value.split(",")[0].toLowerCase())
				.mapValues(value -> value.split(",")[1].toLowerCase());

		// Step3: Pass it to the intermediary stream topic.
		usersAndColours.to("user-keys-and-colours");

		// step4: Read that topic as a KTable so that updates are read correctly
		KTable<String, String> usersAndColoursTable = streamsBuilder.table("user-keys-and-colours");

		// Step5: Count occurrence from the topic
		KTable<String, Long> favouriteColours = usersAndColoursTable
				.groupBy((user, colour) -> new KeyValue<>(colour, colour))
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
						.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

		// Step6: Publish to Kafka topic
		favouriteColours.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

		return streamsBuilder.build();

	}

}
