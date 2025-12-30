package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;
import reactor.core.publisher.Mono;
import scala.Int;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "dehm")
public class DefaultErrorHandlerMonoTest {
	@Test
	void testAckAfterHandlerAsync(@Autowired Config config,
								  @Autowired KafkaTemplate<Integer, String> template)
			throws InterruptedException {

		for (int i = 0; i < 6; i++) {
			template.send("dehm", 0, null, "message contents");
		}
		assertThat(config.latch.await(10, TimeUnit.SECONDS))
				.describedAs("CountDownLatch.count=%d", config.latch.getCount())
				.isTrue();

		for (int i = 0; i < 6; i++) {
			assertThat(config.offsetCountMap.containsKey(i)).isTrue();
			if (i == 1) {
				assertThat(config.offsetCountMap.get(1)).isEqualTo(3);
			} else {
				assertThat(config.offsetCountMap.get(i)).isEqualTo(1);
			}
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {
		private final CountDownLatch latch = new CountDownLatch(8);
		private final Map<Long, Integer> offsetCountMap = new ConcurrentHashMap<>();

		@KafkaListener(id = "dehm.id", topics = "dehm")
		public Mono<Void> onTestTopic(ConsumerRecord<byte[], byte[]> record) {
			long offset = record.offset();
			System.out.println("## offset"+offset);
			Integer count = offsetCountMap.getOrDefault(offset, 0);
			offsetCountMap.put(offset, ++count);

			this.latch.countDown();
			if (record.offset() == 1) {
				throw new RuntimeException("Exception for error handler");
			}
			return Mono.empty();
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1L, 3L)));
			factory.getContainerProperties().setCommitLogLevel(LogIfLevelEnabled.Level.TRACE);
			return factory;
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps("dehm.grp", "false", broker);
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
			return new DefaultKafkaConsumerFactory<>(
					props);
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.producerProps(broker);
			props.put(ProducerConfig.LINGER_MS_CONFIG, 100L);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}
	}
}
