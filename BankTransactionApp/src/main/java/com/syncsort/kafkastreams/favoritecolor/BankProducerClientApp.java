package com.syncsort.kafkastreams.favoritecolor;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Class will produce multiple user records to a Kafka topic.
 * 
 * @author Susmit.Sarkar
 *
 */
public class BankProducerClientApp {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.15.100.66:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
		// From Kafka 0.11, ensure we don't push duplicates
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		Producer<String, String> producer = new KafkaProducer<>(properties);

		int i = 0;
		try {
			while (true) {
				System.out.println("Producing batch: " + i);
				try {
					producer.send(newRandomBankTxn("Susmit"));
					Thread.sleep(100);
					producer.send(newRandomBankTxn("Partha"));
					Thread.sleep(100);
					producer.send(newRandomBankTxn("John"));
					Thread.sleep(100);
					i += 1;
				} catch (InterruptedException e) {
					break;
				}
			}
		} finally {
			producer.close();
		}
	}

	public static ProducerRecord<String, String> newRandomBankTxn(String name) {
		// creates an empty json {}
		ObjectNode transaction = JsonNodeFactory.instance.objectNode();

		Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
		Instant now = Instant.now();

		// Write the data to the JSON document
		transaction.put("name", name);
		transaction.put("amount", amount);
		transaction.put("time", now.toString());
		return new ProducerRecord<>("bank-transactions", name, transaction.toString());
	}

}
