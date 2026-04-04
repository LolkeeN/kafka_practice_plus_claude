package com.vasyl.practice.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {"user-event", "another-user-event", "user-event.DLT"})
class KafkaApplicationTests {

	@Test
	void contextLoads() {
	}

}
