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

package org.springframework.cloud.stream.binder.nats.properties;

import io.nats.client.api.AckPolicy;
import io.nats.client.api.DeliverPolicy;

/**
 * NATS consumer properties.
 */
public class NatsConsumerProperties {

	/**
	 * Whether to use JetStream for this consumer.
	 * Default is true for groups, false for anonymous consumers.
	 */
	private Boolean useJetStream;

	/**
	 * Durable name for the consumer.
	 * If set, implies a durable subscription.
	 */
	private String durableName;

	/**
	 * Delivery policy for the consumer.
	 * Default: DeliverPolicy.All
	 */
	private DeliverPolicy deliverPolicy = DeliverPolicy.All;

	/**
	 * Acknowledgment policy for the consumer.
	 * Default: AckPolicy.Explicit
	 */
	private AckPolicy ackPolicy = AckPolicy.Explicit;

	/**
	 * Maximum number of redelivery attempts.
	 * Default: -1 (infinite)
	 */
	private long maxDeliver = -1;

	public Boolean getUseJetStream() {
		return useJetStream;
	}

	public void setUseJetStream(Boolean useJetStream) {
		this.useJetStream = useJetStream;
	}

	public String getDurableName() {
		return durableName;
	}

	public void setDurableName(String durableName) {
		this.durableName = durableName;
	}

	public DeliverPolicy getDeliverPolicy() {
		return deliverPolicy;
	}

	public void setDeliverPolicy(DeliverPolicy deliverPolicy) {
		this.deliverPolicy = deliverPolicy;
	}

	public AckPolicy getAckPolicy() {
		return ackPolicy;
	}

	public void setAckPolicy(AckPolicy ackPolicy) {
		this.ackPolicy = ackPolicy;
	}

	public long getMaxDeliver() {
		return maxDeliver;
	}

	public void setMaxDeliver(long maxDeliver) {
		this.maxDeliver = maxDeliver;
	}
}
