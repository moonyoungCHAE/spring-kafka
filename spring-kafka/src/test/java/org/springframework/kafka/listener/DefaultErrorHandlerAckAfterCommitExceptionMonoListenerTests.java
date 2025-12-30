/*
 * Copyright 2022-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.*;

/**
 * @author Gary Russell
 * @since 2.9
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class DefaultErrorHandlerAckAfterCommitExceptionMonoListenerTests {

	@Test
	void testAckAfterHandlerAsync(@Autowired DefaultErrorHandlerAckAfterCommitExceptionMonoListenerTests.Config config,
								  @Autowired KafkaTemplate<Integer, String> template)
			throws InterruptedException {

		for (int i = 0; i < 3; i++) {
			template.send("dehm2", 0, null, "message contents");
		}
//		assertThat(config.latch.await(10, TimeUnit.SECONDS))
//				.describedAs("CountDownLatch.count=%d", config.latch.getCount())
//				.isTrue();

		Thread.sleep(1000);

		verify(this.consumer).commitSync(
				Collections.singletonMap(new TopicPartition("dehm2", 0), new OffsetAndMetadata(1L)),
				Duration.ofSeconds(60));

//		for (int i = 0; i < 6; i++) {
//			assertThat(config.offsetCountMap.containsKey(i)).isTrue();
//			if (i == 1) {
//				assertThat(config.offsetCountMap.get(1)).isEqualTo(2);
//			} else {
//				assertThat(config.offsetCountMap.get(i)).isEqualTo(1);
//			}
//		}

	}

	@Configuration
	@EnableKafka
	public static class Config {
		private final CountDownLatch latch = new CountDownLatch(4);
		private final Map<Integer, Integer> offsetCountMap = new ConcurrentHashMap<>();

		@KafkaListener(id = "dehm.id", topics = "dehm")
		public Mono<Void> onTestTopic(ConsumerRecord<byte[], byte[]> record) {
			long offset = record.offset();
			Integer count = offsetCountMap.getOrDefault((int) offset, 0);
			offsetCountMap.put((int) offset, ++count);
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
			Map<String, Object> props = KafkaTestUtils.consumerProps("dehm2.grp", "false", broker);
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
