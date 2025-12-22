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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * NATS binder configuration properties.
 */
@ConfigurationProperties(prefix = "spring.cloud.stream.nats.binder")
public class NatsBinderConfigurationProperties {

	/**
	 * NATS server URLs.
	 * Default: nats://localhost:4222
	 */
	private String servers = "nats://localhost:4222";

	/**
	 * Username for authentication.
	 */
	private String username;

	/**
	 * Password for authentication.
	 */
	private String password;

	/**
	 * Authentication token.
	 */
	private String token;

	/**
	 * Credential file path.
	 */
	private String credentialPath;

	/**
	 * Connection name.
	 */
	private String connectionName;

	/**
	 * Max reconnect attempts.
	 * Default: -1 (infinite)
	 */
	private int maxReconnects = -1;

	/**
	 * Reconnect wait time in milliseconds.
	 * Default: 2000 (2s)
	 */
	private long reconnectWait = 2000;

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getCredentialPath() {
		return credentialPath;
	}

	public void setCredentialPath(String credentialPath) {
		this.credentialPath = credentialPath;
	}

	public String getConnectionName() {
		return connectionName;
	}

	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}

	public int getMaxReconnects() {
		return maxReconnects;
	}

	public void setMaxReconnects(int maxReconnects) {
		this.maxReconnects = maxReconnects;
	}

	public long getReconnectWait() {
		return reconnectWait;
	}

	public void setReconnectWait(long reconnectWait) {
		this.reconnectWait = reconnectWait;
	}
}
