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

package org.springframework.cloud.stream.binder.nats.provisioning;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsConsumerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;

/**
 * NATS provisioner.
 */
public class NatsProvisioner implements
		ProvisioningProvider<ExtendedConsumerProperties<NatsConsumerProperties>, ExtendedProducerProperties<NatsProducerProperties>> {

	private final Connection connection;

	public NatsProvisioner(Connection connection) {
		this.connection = connection;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ExtendedProducerProperties<NatsProducerProperties> properties)
			throws ProvisioningException {
		
		if (properties.getExtension().isUseJetStream()) {
			String streamName = properties.getExtension().getStreamName();
			if (streamName == null) {
				// Default stream naming convention if not provided? 
				// Or require it? For now, let's assume if it's not provided, we might create one based on destination?
				// But best practice is to have stream defined. 
				// If streamName is null, we can try to find a stream that has this subject.
			}
			else {
				// Verify stream exists or create it?
				// For this simple implementation, we'll assume stream might need creation if configured, 
				// but more likely we just verify it exists if we want to be strict.
				try {
					JetStreamManagement jsm = connection.jetStreamManagement();
					if (!jsm.getStreamNames().contains(streamName)) {
						// Create stream with default config matching subject?
						// This is a complex decision. For now, let's just log or error if strict.
						// Let's create a stream for the subject if it doesn't exist and we want auto-provisioning.
						StreamConfiguration sc = StreamConfiguration.builder()
								.name(streamName)
								.subjects(name)
								.build();
						jsm.addStream(sc);
					}
					else {
						// Check if subject is bound to stream?
						StreamInfo si = jsm.getStreamInfo(streamName);
						if (!si.getConfiguration().getSubjects().contains(name)) {
							// Add subject to stream? 
							// jsm.updateStream(...)
						}
					}
				}
				catch (Exception e) {
					throw new ProvisioningException("Failed to provision NATS stream", e);
				}
			}
		}

		return new NatsProducerDestination(name);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name,String group,
			ExtendedConsumerProperties<NatsConsumerProperties> properties)
			throws ProvisioningException {
		return new NatsConsumerDestination(name);
	}

	private static class NatsProducerDestination implements ProducerDestination {

		private final String name;

		NatsProducerDestination(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.name;
		}
	}

	private static class NatsConsumerDestination implements ConsumerDestination {

		private final String name;

		NatsConsumerDestination(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return this.name;
		}
	}
}
