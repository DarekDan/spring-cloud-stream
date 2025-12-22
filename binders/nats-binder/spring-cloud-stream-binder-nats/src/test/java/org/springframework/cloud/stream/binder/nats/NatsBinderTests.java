/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.cloud.stream.binder.nats;

import java.nio.charset.StandardCharsets;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsConsumerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsProducerProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class NatsBinderTests {

	@Container
	private static final NatsTestContainer natsContainer = new NatsTestContainer();

	private static Connection connection;
	private static NatsTestBinder binder;

	@BeforeAll
	public static void setup() throws Exception {
		Options options = new Options.Builder().server(natsContainer.getNatsUrl()).build();
		connection = Nats.connect(options);
		binder = new NatsTestBinder(connection);
	}

	@AfterAll
	public static void tearDown() throws Exception {
		if (connection != null) {
			connection.close();
		}
	}

	@Test
	void testBasicPubSub() throws Exception {
		// Producer
		DirectChannel output = new DirectChannel();
		ExtendedProducerProperties<NatsProducerProperties> producerProps = 
				new ExtendedProducerProperties<>(new NatsProducerProperties());
		Binding<MessageChannel> producerBinding = binder.bindProducer("test-subject", output, producerProps);

		// Consumer
		DirectChannel input = new DirectChannel();
		ExtendedConsumerProperties<NatsConsumerProperties> consumerProps = 
				new ExtendedConsumerProperties<>(new NatsConsumerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("test-subject", "test-group", input, consumerProps);

		// Test
		String testPayload = "hello nats";
		output.send(MessageBuilder.withPayload(testPayload.getBytes(StandardCharsets.UTF_8)).build());


		// We usually use a specialized channel or a countdown latch handler on the input channel.
		
		// Let's reimplement with a subscriber on 'input' channel.
		java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
		java.util.concurrent.atomic.AtomicReference<Message<?>> receivedInfo = new java.util.concurrent.atomic.AtomicReference<>();
		
		input.subscribe(message -> {
			receivedInfo.set(message);
			latch.countDown();
		});

		// Resend to ensure subscriber gets it (if timing was off)
		output.send(MessageBuilder.withPayload(testPayload.getBytes(StandardCharsets.UTF_8)).build());
		
		boolean received = latch.await(5, java.util.concurrent.TimeUnit.SECONDS);
		assertThat(received).isTrue();
		assertThat(receivedInfo.get()).isNotNull();
		assertThat(new String((byte[]) receivedInfo.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload);

		producerBinding.unbind();
		consumerBinding.unbind();
	}
}
