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

import java.time.Duration;

/**
 * NATS producer properties.
 */
public class NatsProducerProperties {

	/**
	 * Whether to use JetStream for publishing.
	 * Default: false (Core NATS)
	 */
	private boolean useJetStream = false;

	/**
	 * Name of the stream to publish to.
	 * Required if using JetStream and stream doesn't exist.
	 */
	private String streamName;

	/**
	 * Timeout for publish acknowledgment.
	 * Default: 2 seconds
	 */
	private Duration ackTimeout = Duration.ofSeconds(2);

	public boolean isUseJetStream() {
		return useJetStream;
	}

	public void setUseJetStream(boolean useJetStream) {
		this.useJetStream = useJetStream;
	}

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public Duration getAckTimeout() {
		return ackTimeout;
	}

	public void setAckTimeout(Duration ackTimeout) {
		this.ackTimeout = ackTimeout;
	}
}
