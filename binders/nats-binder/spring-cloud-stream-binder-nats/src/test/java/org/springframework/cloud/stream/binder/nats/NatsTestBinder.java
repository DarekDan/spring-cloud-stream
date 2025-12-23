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
import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.nats.config.NatsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsConsumerProperties;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.nats.properties.NatsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsProducerProperties;
import org.springframework.cloud.stream.binder.nats.provisioning.NatsProvisioner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.messaging.MessageChannel;

/**
 * NATS Test Binder.
 */
public class NatsTestBinder extends
		AbstractTestBinder<NatsMessageChannelBinder, ExtendedConsumerProperties<NatsConsumerProperties>, ExtendedProducerProperties<NatsProducerProperties>> {

	public NatsTestBinder(Connection connection) {
		NatsMessageChannelBinder binder = new NatsMessageChannelBinder(new NatsExtendedBindingProperties(),
				new NatsProvisioner(connection),
				connection);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();
		binder.setApplicationContext(context);
		((org.springframework.beans.factory.BeanFactoryAware) binder).setBeanFactory(context.getBeanFactory());
		try {
			binder.afterPropertiesSet();
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize binder", e);
		}
		this.setBinder(binder);
	}

	@Override
	public void cleanup() {
		// No-op for now?
	}

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel moduleInputChannel,
			ExtendedConsumerProperties<NatsConsumerProperties> properties) {
		return getBinder().bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel,
			ExtendedProducerProperties<NatsProducerProperties> properties) {
		return getBinder().bindProducer(name, moduleOutputChannel, properties);
	}
}
