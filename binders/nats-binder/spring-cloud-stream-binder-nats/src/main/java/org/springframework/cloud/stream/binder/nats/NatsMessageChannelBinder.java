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

import java.io.IOException;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.PushSubscribeOptions;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.nats.config.NatsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsConsumerProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.nats.properties.NatsProducerProperties;
import org.springframework.cloud.stream.binder.nats.provisioning.NatsProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

/**
 * NATS Binder implementation.
 */
public class NatsMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<NatsConsumerProperties>, ExtendedProducerProperties<NatsProducerProperties>, NatsProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, NatsConsumerProperties, NatsProducerProperties>,
		BeanFactoryAware {

	private final NatsExtendedBindingProperties extendedBindingProperties;
	private final Connection connection;
	private BeanFactory beanFactory;

	public NatsMessageChannelBinder(NatsExtendedBindingProperties extendedBindingProperties,
			NatsBinderConfigurationProperties natsProperties,
			NatsProvisioner provisioningProvider,
			Connection connection) {
		super(new String[0], provisioningProvider);
		this.extendedBindingProperties = extendedBindingProperties;
		this.connection = connection;
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<NatsProducerProperties> producerProperties,
			MessageChannel errorChannel) throws Exception {
		NatsMessageHandler handler = new NatsMessageHandler(destination.getName(), producerProperties, connection);
		if (this.beanFactory != null) {
			handler.setBeanFactory(this.beanFactory);
		}
		return handler;
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination,
			String group,
			ExtendedConsumerProperties<NatsConsumerProperties> properties) throws Exception {
		NatsMessageProducer producer = new NatsMessageProducer(destination.getName(), group, properties, connection);
		if (this.beanFactory != null) {
			producer.setBeanFactory(this.beanFactory);
		}
		return producer;
	}

	@Override
	public NatsConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public NatsProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	/**
	 * NATS Message Handler for sending messages.
	 */
	private static class NatsMessageHandler extends AbstractMessageHandler {

		private final String subject;
		private final ExtendedProducerProperties<NatsProducerProperties> properties;
		private final Connection connection;
		private final JetStream jetStream;

		public NatsMessageHandler(String subject,
				ExtendedProducerProperties<NatsProducerProperties> properties,
				Connection connection) throws IOException {
			this.subject = subject;
			this.properties = properties;
			this.connection = connection;
			this.jetStream = properties.getExtension().isUseJetStream() ? connection.jetStream() : null;
		}

		@Override
		protected void handleMessageInternal(org.springframework.messaging.Message<?> message) {
			try {
				if (jetStream != null) {
					jetStream.publish(subject, (byte[]) message.getPayload());
				} else {
					connection.publish(subject, (byte[]) message.getPayload());
				}
			} catch (Exception e) {
				throw new MessagingException(message, "Failed to publish to NATS subject: " + subject, e);
			}
		}
	}

	/**
	 * NATS Message Producer for receiving messages.
	 */
	private static class NatsMessageProducer extends MessageProducerSupport {

		private final String subject;
		private final String group;
		private final ExtendedConsumerProperties<NatsConsumerProperties> properties;
		private final Connection connection;
		private Dispatcher dispatcher;

		public NatsMessageProducer(String subject, String group,
				ExtendedConsumerProperties<NatsConsumerProperties> properties,
				Connection connection) {
			this.subject = subject;
			this.group = group;
			this.properties = properties;
			this.connection = connection;
		}

		@Override
		protected void onInit() {
			super.onInit();
		}

		@Override
		protected void doStart() {
			try {
				if (Boolean.TRUE.equals(properties.getExtension().getUseJetStream())
						|| (group != null && properties.getExtension().getUseJetStream() == null)) {
					// Use JetStream
					JetStream js = connection.jetStream();
					PushSubscribeOptions.Builder optionsBuilder = PushSubscribeOptions.builder();

					if (group != null) {
						optionsBuilder.durable(properties.getExtension().getDurableName() != null
								? properties.getExtension().getDurableName()
								: group);
					}

					// Configure consumer options...
					// This is a simplified implementation

					this.dispatcher = connection.createDispatcher();
					js.subscribe(subject, group, this.dispatcher, this::handleNatsMessage, false,
							optionsBuilder.build());
				} else {
					// core NATS
					this.dispatcher = connection.createDispatcher(this::handleNatsMessage);
					if (group != null) {
						this.dispatcher.subscribe(subject, group);
					} else {
						this.dispatcher.subscribe(subject);
					}
				}
			} catch (Exception e) {
				throw new MessagingException("Failed to start NATS consumer", e);
			}
		}

		@Override
		protected void doStop() {
			if (this.dispatcher != null) {
				this.connection.closeDispatcher(this.dispatcher);
			}
		}

		private void handleNatsMessage(Message msg) {
			try {
				sendMessage(MessageBuilder.withPayload(msg.getData())
						.setHeader("nats_subject", msg.getSubject())
						.setHeader("nats_replyTo", msg.getReplyTo())
						.build());
				msg.ack();
			} catch (Exception e) {
				// Log error
				logger.error(e, "Failed to process NATS message");
				msg.nak();
			}
		}
	}
}
