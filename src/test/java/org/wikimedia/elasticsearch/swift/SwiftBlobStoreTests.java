/*
 * Copyright 2017 Wikimedia and BigData Boutique
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.elasticsearch.swift;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.javaswift.joss.client.mock.AccountMock;
import org.javaswift.joss.swift.Swift;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.repositories.blobstore.SwiftBlobStore;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

@RunWith(RandomizedRunner.class)
public class SwiftBlobStoreTests extends ESBlobStoreRepositoryIntegTestCase {

    private Swift swift;
    private AccountMock account;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(SwiftRepositoryPlugin.class);
    }

    @Override
    protected Settings repositorySettings() {
        // FIXME: By providing valid Swift repository settings here, these tests will pass.
        // FIXME: Create the containers in Swift prior to running the test suite.
        return Settings.builder().put(super.repositorySettings()).
                put("swift_url", "http://swift.example.com:8080/auth/v1.0").
                put("swift_container", "es_plugin_tests").
                put("swift_username", "example:example").
                put("swift_password", "example").build();
    }

    @Before
    public void setup() {
        this.swift = new Swift();
        this.account = new AccountMock(swift);
    }

    @Override
    protected String repositoryType() {
        return SwiftRepository.TYPE;
    }

    @Override
    protected BlobStore newBlobStore() {
        String container = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        return new SwiftBlobStore(Settings.EMPTY, this.account, container);
    }
}
