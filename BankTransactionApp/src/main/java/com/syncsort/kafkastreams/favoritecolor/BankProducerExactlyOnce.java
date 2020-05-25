package com.syncsort.kafkastreams.favoritecolor;

import java.time.Instant;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Real time calculation of banking data for multiple users.
 * 
 * @author Susmit.Sarkar
 *
 */
public class BankProducerExactlyOnce {

	private static KafkaStreams streams = null;

	private final static Logger log = Logger.getLogger(BankProducerExactlyOnce.class.getName());

	public static void main(String[] args) {
		Properties config = new Properties();

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.15.100.66:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// Exactly once processing
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		streams = new KafkaStreams(createBuilderTopology(), config);

		// If any exception arises log it.
		streams.setUncaughtExceptionHandler((t, exception) -> {
			log.info(String.format("Stream error on thread: %s", t.getName()) + " due to: " + exception.getMessage());
		});

		streams.start();

		// shutdown hook to correctly close the streams application
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static Topology createBuilderTopology() {
		// JSON Serde to read JSON from Kafka topic.
		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
		final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
		final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, JsonNode> bankTransactionsStream = builder.stream("bank-transactions",
				Consumed.with(Serdes.String(), jsonSerde));

		// Create the initial JSON object for balances to push in the topic
		ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
		initialBalance.put("count", 0);
		initialBalance.put("balance", 0);
		initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

		KTable<String, JsonNode> bankBalance = bankTransactionsStream
				// group by Key which is the name as string (example: "Susmit")
				// Susmit | {"count":336,"balance":15810,"time":"2020-05-25T07:23:05.096Z"}
				.groupByKey(Grouped.with(Serdes.String(), jsonSerde)).aggregate(() -> initialBalance,
						(key, transaction, balance) -> aggregatedBalance(transaction, balance),
						Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
								.withKeySerde(Serdes.String()).withValueSerde(jsonSerde));

		bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonSerde));

		return builder.build();

	}

	// transaction is the JSON Node which we get from "bank-transactions" topic.
	// balance is the previous stored value in "bank-balance-exactly-once" topic
	private static JsonNode aggregatedBalance(JsonNode transaction, JsonNode balance) {
		// create a new balance JSON object
		ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
		// Increment one and add to the JSON object.
		newBalance.put("count", balance.get("count").asInt() + 1);
		newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

		Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
		Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
		Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
		newBalance.put("time", newBalanceInstant.toString());
		return newBalance;
	}

}
