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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.util.Assert;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract base class for JUnit 5 extensions that detect the presence of some
 * external
 * resource. If the resource is indeed present, it will be available during the
 * test
 * lifecycle through {@link #getResource()}. If it is not, tests will either
 * fail or be
 * skipped, depending on the value of system property
 * {@value #SCS_EXTERNAL_SERVERS_REQUIRED}.
 *
 * @param <R> resource type
 */
public abstract class AbstractExternalResourceTestSupport<R> implements BeforeEachCallback {

    /**
     * SCS external servers required environment variable.
     */
    public static final String SCS_EXTERNAL_SERVERS_REQUIRED = "SCS_EXTERNAL_SERVERS_REQUIRED";

    protected final Log logger = LogFactory.getLog(getClass());

    protected R resource;

    private String resourceDescription;

    protected AbstractExternalResourceTestSupport(String resourceDescription) {
        Assert.hasText(resourceDescription, "resourceDescription is required");
        this.resourceDescription = resourceDescription;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        try {
            obtainResource();
        } catch (Exception e) {
            maybeCleanup();
            // Assume true means we proceed if available, but here we want to FAIL if
            // required
            // or simply log and skip if not required?
            // The original code does Assertions.fail().
            // But usually we want to 'skip' if not present, unless
            // SCS_EXTERNAL_SERVERS_REQUIRED is set.

            // Original Rabbit logic:
            // Assertions.fail() - this fails the test!
            // Wait, if it fails, then test fails.
            // Let's check if the original intended to skip.
            // "If it is not, tests will either fail or be skipped" description says.
            // But code says `Assertions.fail()`.

            // Let's implement logic to check property.
            String required = System.getProperty(SCS_EXTERNAL_SERVERS_REQUIRED);
            if ("true".equalsIgnoreCase(required)) {
                fail("External server " + resourceDescription + " required but not available: " + e.getMessage());
            } else {
                // To skip in JUnit 5, uses Assumptions
                org.junit.jupiter.api.Assumptions.assumeTrue(false,
                        "External server " + resourceDescription + " not available");
            }
        }
    }

    private void maybeCleanup() {
        if (this.resource != null) {
            try {
                cleanupResource();
            } catch (Exception ignored) {
                this.logger.warn("Exception while trying to cleanup failed resource",
                        ignored);
            }
        }
    }

    public R getResource() {
        return this.resource;
    }

    protected abstract void cleanupResource() throws Exception;

    protected abstract void obtainResource() throws Exception;

}
