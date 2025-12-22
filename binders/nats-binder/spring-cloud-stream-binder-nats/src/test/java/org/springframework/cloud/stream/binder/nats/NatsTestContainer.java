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

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * NATS Test Container.
 */
public class NatsTestContainer extends GenericContainer<NatsTestContainer> {

	private static final int NATS_PORT = 4222;
	private static final int MONITOR_PORT = 8222;

	public NatsTestContainer() {
		super("nats:latest");
		withExposedPorts(NATS_PORT, MONITOR_PORT);
		withCommand("-js"); // Enable JetStream
		waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));
		withStartupTimeout(Duration.ofMinutes(2));
	}

	public String getNatsUrl() {
		return "nats://" + getHost() + ":" + getMappedPort(NATS_PORT);
	}
}
