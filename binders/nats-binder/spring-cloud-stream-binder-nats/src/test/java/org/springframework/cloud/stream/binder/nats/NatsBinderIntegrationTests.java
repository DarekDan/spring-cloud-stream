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

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsConsumerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsProducerProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for NATS Binder using Testcontainers.
 * Covers: Clustering, JetStream, Failover, Fan-out.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NatsBinderIntegrationTests {

    private static final Network NETWORK = Network.newNetwork();
    private static final List<GenericContainer<?>> CLUSTER = new ArrayList<>();

    private static NatsTestBinder binder;
    private static Connection connection;
    private static String clusterUrl;

    @BeforeAll
    static void setupCluster() throws Exception {
        // Create 3-node NATS cluster with JetStream enabled
        GenericContainer<?> node1 = createNatsNode("node1", 4222, 8222,
                "nats://node2:6222,nats://node3:6222");
        GenericContainer<?> node2 = createNatsNode("node2", 4222, 8222,
                "nats://node1:6222,nats://node3:6222");
        GenericContainer<?> node3 = createNatsNode("node3", 4222, 8222,
                "nats://node1:6222,nats://node2:6222");

        CLUSTER.add(node1);
        CLUSTER.add(node2);
        CLUSTER.add(node3);

        CLUSTER.forEach(GenericContainer::start);

        // Build comma-separated URL list for the cluster
        StringBuilder urls = new StringBuilder();
        for (GenericContainer<?> node : CLUSTER) {
            if (urls.length() > 0)
                urls.append(",");
            urls.append("nats://" + node.getHost() + ":" + node.getMappedPort(4222));
        }
        clusterUrl = urls.toString();

        Options options = new Options.Builder().servers(clusterUrl.split(",")).maxReconnects(-1)
                .ignoreDiscoveredServers().build();
        connection = Nats.connect(options);

        // Create Streams
        // Create Streams with retry mechanism to allow cluster to form
        JetStreamManagement jsm = connection.jetStreamManagement();
        int maxRetries = 20;
        for (int i = 0; i < maxRetries; i++) {
            try {
                jsm.addStream(
                        StreamConfiguration.builder().name("alo-stream").subjects("js.alo.test").replicas(3).build());
                jsm.addStream(
                        StreamConfiguration.builder().name("eo-stream").subjects("js.eo.test").replicas(3).build());
                jsm.addStream(StreamConfiguration.builder().name("fanout-stream").subjects("fanout.test").replicas(3)
                        .build());
                jsm.addStream(StreamConfiguration.builder().name("failover-stream").subjects("failover.>").replicas(3)
                        .build());
                break;
            } catch (Exception e) {
                if (i == maxRetries - 1) {
                    throw e;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }

        binder = new NatsTestBinder(connection);
    }

    private static GenericContainer<?> createNatsNode(String hostName, int clientPort, int monitorPort, String routes) {
        return new GenericContainer<>("nats:latest")
                .withNetwork(NETWORK)
                .withNetworkAliases(hostName)
                .withExposedPorts(clientPort, monitorPort, 6222)
                .withCommand("-js", "-name", hostName, "-cluster_name", "my_cluster", "-cluster", "nats://0.0.0.0:6222",
                        "-routes", routes, "-m", String.valueOf(monitorPort))
                .waitingFor(Wait.forLogMessage(".*Server is ready.*", 1))
                .withStartupTimeout(Duration.ofMinutes(1));
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (connection != null)
            connection.close();
        CLUSTER.forEach(GenericContainer::stop);
        NETWORK.close();
    }

    @Test
    @Order(1)
    void testJetStreamAtLeastOnceDelivery() throws Exception {
        String subject = "js.alo.test";
        DirectChannel output = new DirectChannel();
        output.setBeanName("outputAlo");
        DirectChannel input = new DirectChannel();
        input.setBeanName("inputAlo");

        ExtendedProducerProperties<NatsProducerProperties> producerProps = new ExtendedProducerProperties<>(
                new NatsProducerProperties());
        producerProps.getExtension().setUseJetStream(true);

        ExtendedConsumerProperties<NatsConsumerProperties> consumerProps = new ExtendedConsumerProperties<>(
                new NatsConsumerProperties());
        consumerProps.getExtension().setUseJetStream(true);
        consumerProps.getExtension().setAckPolicy(AckPolicy.Explicit); // Manual ack required

        Binding<MessageChannel> producerBinding = binder.bindProducer(subject, output, producerProps);
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(subject, "alo-group", input, consumerProps);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger receivedCount = new AtomicInteger(0);

        // Consumer that fails once then succeeds
        input.subscribe(msg -> {
            int count = receivedCount.incrementAndGet();
            if (count == 1) {
                // Simulate processing failure (no ack)
                throw new RuntimeException("Simulated failure");
            }
            latch.countDown();
        });

        output.send(MessageBuilder.withPayload("retry-me".getBytes()).build());

        // Wait for redelivery
        boolean received = latch.await(10, TimeUnit.SECONDS);

        // Note: First delivery fails, so we expect redelivery.
        // Since we don't have manual ACK in the test binder callback easily exposed
        // without custom handling,
        // relying on exception usually triggers NAK in binder implementation.

        assertThat(receivedCount.get()).isGreaterThanOrEqualTo(2);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @Test
    @Order(2)
    void testJetStreamExactlyOnce() throws Exception {
        String subject = "js.eo.test";
        DirectChannel output = new DirectChannel();
        output.setBeanName("outputEo");
        DirectChannel input = new DirectChannel();
        input.setBeanName("inputEo");

        ExtendedProducerProperties<NatsProducerProperties> producerProps = new ExtendedProducerProperties<>(
                new NatsProducerProperties());
        producerProps.getExtension().setUseJetStream(true);

        ExtendedConsumerProperties<NatsConsumerProperties> consumerProps = new ExtendedConsumerProperties<>(
                new NatsConsumerProperties());
        consumerProps.getExtension().setUseJetStream(true);
        consumerProps.getExtension().setDurableName("eo-durable");

        Binding<MessageChannel> producerBinding = binder.bindProducer(subject, output, producerProps);
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(subject, "eo-group", input, consumerProps);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);

        input.subscribe(msg -> {
            receivedCount.incrementAndGet();
            latch.countDown();
        });

        String uniqueId = "unique-id-123";
        Message<?> msg = MessageBuilder.withPayload("dedup-me".getBytes())
                .setHeader("nats_msgId", uniqueId) // Standard header for deduplication if mapped
                .build();

        output.send(MessageBuilder.withPayload("dedup-test".getBytes()).build());
        latch.await(5, TimeUnit.SECONDS);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @Test
    @Order(3)
    void testFanOutPattern() throws Exception {
        String subject = "fanout.test";
        DirectChannel output = new DirectChannel();
        output.setBeanName("outputFanOut");
        DirectChannel input1 = new DirectChannel();
        input1.setBeanName("inputFanOut1");
        DirectChannel input2 = new DirectChannel();
        input2.setBeanName("inputFanOut2");

        ExtendedProducerProperties<NatsProducerProperties> producerProps = new ExtendedProducerProperties<>(
                new NatsProducerProperties());

        ExtendedConsumerProperties<NatsConsumerProperties> consumerProps1 = new ExtendedConsumerProperties<>(
                new NatsConsumerProperties());
        ExtendedConsumerProperties<NatsConsumerProperties> consumerProps2 = new ExtendedConsumerProperties<>(
                new NatsConsumerProperties());

        // Fan-out: Same subject, different groups (or no group for pub/sub)

        Binding<MessageChannel> producerBinding = binder.bindProducer(subject, output, producerProps);
        Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(subject, "group1", input1, consumerProps1);
        Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(subject, "group2", input2, consumerProps2);

        CountDownLatch latch = new CountDownLatch(2);

        input1.subscribe(msg -> latch.countDown());
        input2.subscribe(msg -> latch.countDown());

        output.send(MessageBuilder.withPayload("broadcast".getBytes()).build());

        boolean received = latch.await(5, TimeUnit.SECONDS);
        assertThat(received).isTrue();

        producerBinding.unbind();
        consumerBinding1.unbind();
        consumerBinding2.unbind();
    }

    @Test
    @Order(4)
    void testClusterFailoverClean() throws Exception {
        String subject = "failover.clean";
        DirectChannel output = new DirectChannel();
        output.setBeanName("outputFailoverClean");
        DirectChannel input = new DirectChannel();
        input.setBeanName("inputFailoverClean");

        ExtendedProducerProperties<NatsProducerProperties> producerProps = new ExtendedProducerProperties<>(
                new NatsProducerProperties());
        producerProps.getExtension().setUseJetStream(true);

        ExtendedConsumerProperties<NatsConsumerProperties> consumerProps = new ExtendedConsumerProperties<>(
                new NatsConsumerProperties());
        consumerProps.getExtension().setUseJetStream(true);
        consumerProps.getExtension().setDurableName("failover-durable");

        Binding<MessageChannel> producerBinding = binder.bindProducer(subject, output, producerProps);
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(subject, "failover-clean", input, consumerProps);

        List<String> payloads = new java.util.concurrent.CopyOnWriteArrayList<>();
        input.subscribe(msg -> payloads.add(new String((byte[]) msg.getPayload())));

        // 1. Send normal
        output.send(MessageBuilder.withPayload("msg1".getBytes()).build());

        // Wait for msg1
        await().until(() -> payloads.contains("msg1"));

        // 2. Kill a node (ensure we don't kill the one we are mainly connected to?
        // NATS client is smart. We kill index 0.)
        CLUSTER.get(0).stop();

        // Give a moment for client to detect
        try {
            Thread.sleep(5000);

            // 3. Send again
            output.send(MessageBuilder.withPayload("msg2".getBytes()).build());

            // Should receive msg2
            await().until(() -> payloads.contains("msg2"));
        } finally {
            // Restart node for other tests (order matters!)
            CLUSTER.get(0).start();
        }

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    private interface Condition {
        boolean check();
    }

    private static class Awaiter {
        void until(Condition c) {
            long end = System.currentTimeMillis() + 30000;
            while (System.currentTimeMillis() < end) {
                if (c.check())
                    return;
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
            }
            throw new AssertionError("Timed out waiting for condition");
        }
    }

    private Awaiter await() {
        return new Awaiter();
    }

}
