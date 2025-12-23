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

package org.springframework.cloud.stream.binder.test.junit.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

/**
 * JUnit 5 extension that detects the fact that NATS is available on localhost.
 */
public class NatsTestSupport extends AbstractExternalResourceTestSupport<Connection> {

    private final String natsUrl;

    public NatsTestSupport() {
        this(Options.DEFAULT_URL);
    }

    public NatsTestSupport(String natsUrl) {
        super("NATS");
        this.natsUrl = natsUrl;
    }

    @Override
    protected void obtainResource() throws Exception {
        Options options = new Options.Builder().server(this.natsUrl).build();
        this.resource = Nats.connect(options);
        // Verify connection
        if (this.resource.getStatus() != Connection.Status.CONNECTED) {
            this.resource.close();
            throw new IllegalStateException("Could not connect to NATS at " + this.natsUrl);
        }
    }

    @Override
    protected void cleanupResource() throws Exception {
        if (this.resource != null) {
            this.resource.close();
        }
    }

}
