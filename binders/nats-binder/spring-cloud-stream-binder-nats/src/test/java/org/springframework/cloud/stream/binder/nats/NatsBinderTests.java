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

@Testcontainers(disabledWithoutDocker = true)
public class NatsBinderTests {

	// Remove @Container annotation to control lifecycle manually if needed, 
	// OR rely on @Testcontainers(disabledWithoutDocker = true) if available in this validation.
	// However, disabledWithoutDocker is a feature of the extension. 
	// Let's stick to the manual check for maximum reliability as the extension might still fail initialization.
	
	@Container
	private static final NatsTestContainer natsContainer = new NatsTestContainer();

	private static Connection connection;
	private static NatsTestBinder binder;

	@BeforeAll
	public static void setup() throws Exception {
		// The @Testcontainers extension handles startup.
		// If we want to strictly skip if docker is missing without failing the build, 
		// we might need to rely on the annotation or manual check.
		// The error "IllegalStateException: Could not find a valid Docker environment" comes from the extension or init.
		
		// To fix the hard crash: We need to assume Docker is available BEFORE the container tries to start.
		// But static fields init runs early. 
		
		// Let's assume the user DOES want to run tests if possible, but skip if not.
		// The most robust way is manual start in a static block or BeforeAll with checks.
		
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
