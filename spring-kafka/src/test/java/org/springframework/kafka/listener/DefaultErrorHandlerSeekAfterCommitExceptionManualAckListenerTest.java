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
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "dehma")
public class DefaultErrorHandlerSeekAfterCommitExceptionManualAckListenerTest {
	public static Map<Integer, Integer> offsetCountMap =  new ConcurrentHashMap<>();


	@Test
	void testErrHandlerMonoListener(@Autowired Config config,
									@Autowired KafkaTemplate<Integer, String> template)
			throws InterruptedException {

		for (int i = 0; i < 6; i++) {
			template.send("dehma", 0, null, "message contents");
		}
		assertThat(config.latch.await(10, TimeUnit.SECONDS))
				.describedAs("CountDownLatch.count=%d", config.latch.getCount())
				.isTrue();

		for (int i = 0; i < 6; i++) {
			assertThat(offsetCountMap.containsKey(i)).isTrue();
			if (i == 1) {
				assertThat(offsetCountMap.get(1)).isEqualTo(2);
			} else {
				assertThat(offsetCountMap.get(i)).isEqualTo(1);
			}
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch = new CountDownLatch(7);
		int exceptionCount = 0;

		@KafkaListener(id = "dehma.id", topics = "dehma")
		public void onTestTopic(final ConsumerRecord<byte[], byte[]> record,
								final Acknowledgment acknowledgment) {
			accept(record, acknowledgment);
		}

		private void accept(final ConsumerRecord<byte[], byte[]> record,
							final Acknowledgment acknowledgment) {
			System.out.println("## listener offset "+record.offset());
			int offsetCount = offsetCountMap.getOrDefault((int) record.offset(), 0);
			offsetCount += 1;
			offsetCountMap.put((int) record.offset(), offsetCount);

			if (record.offset() == 1) {
				if (exceptionCount == 1) {
					this.latch.countDown();
					acknowledgment.acknowledge();
					return;
				}
				System.out.println("## exception from listener");
				exceptionCount++;
				this.latch.countDown();
				throw new RuntimeException("Exception for error handler");
			} else {
				this.latch.countDown();
				acknowledgment.acknowledge();
			}
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setCommonErrorHandler(new DefaultErrorHandler());
			factory.getContainerProperties().setAsyncAcks(true);
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
			factory.getContainerProperties().setCommitLogLevel(LogIfLevelEnabled.Level.TRACE);
			return factory;
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps("dehma.grp", "false", broker);
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
