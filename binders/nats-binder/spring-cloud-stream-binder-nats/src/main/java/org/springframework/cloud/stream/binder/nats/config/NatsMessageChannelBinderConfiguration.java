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

package org.springframework.cloud.stream.binder.nats.config;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.nats.NatsMessageChannelBinder;
import org.springframework.cloud.stream.binder.nats.properties.NatsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.nats.provisioning.NatsProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * NATS Message Channel Binder Configuration.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ NatsBinderConfigurationProperties.class, NatsExtendedBindingProperties.class })
public class NatsMessageChannelBinderConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public Connection natsConnection(NatsBinderConfigurationProperties properties) throws IOException, InterruptedException {
		Options.Builder builder = new Options.Builder()
				.servers(properties.getServers().split(","))
				.connectionName(properties.getConnectionName())
				.maxReconnects(properties.getMaxReconnects())
				.reconnectWait(java.time.Duration.ofMillis(properties.getReconnectWait()));

		if (properties.getUsername() != null && properties.getPassword() != null) {
			builder.userInfo(properties.getUsername(), properties.getPassword());
		}
		else if (properties.getToken() != null) {
			builder.token(properties.getToken());
		}
		else if (properties.getCredentialPath() != null) {
			builder.authHandler(Nats.credentials(properties.getCredentialPath()));
		}

		return Nats.connect(builder.build());
	}

	@Bean
	@ConditionalOnMissingBean
	public NatsProvisioner natsProvisioner(Connection connection) {
		return new NatsProvisioner(connection);
	}

	@Bean
	@ConditionalOnMissingBean
	public NatsMessageChannelBinder natsMessageChannelBinder(
			NatsExtendedBindingProperties extendedBindingProperties,
			NatsBinderConfigurationProperties natsProperties,
			NatsProvisioner natsProvisioner,
			Connection connection) {
		
		return new NatsMessageChannelBinder(extendedBindingProperties,
				natsProperties, natsProvisioner, connection);
	}
}
