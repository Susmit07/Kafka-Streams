package com.syncsort.kafkastreams.wordcount;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncsort.kafkastreams.favoritecolor.BankProducerClientApp;

/**
 * Unit test for simple {@link BankProducerClientApp}.
 */
public class AppTest {

	@Test
	public void newRandomTransactionsTest() {
		ProducerRecord<String, String> producerRecord = BankProducerClientApp.newRandomBankTxn("Susmit");
		String key = producerRecord.key();
		String value = producerRecord.value();

		assertEquals(key, "Susmit");

		// {"name":"Susmit","amount":36,"time":"2020-05-25T05:25:06.297Z"}

		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(value);
			assertEquals(node.get("name").asText(), "Susmit");
			assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
