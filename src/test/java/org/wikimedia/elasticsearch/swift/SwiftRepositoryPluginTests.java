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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.javaswift.joss.client.mock.AccountMock;
import org.javaswift.joss.client.mock.ContainerMock;
import org.javaswift.joss.client.mock.StoredObjectMock;
import org.javaswift.joss.instructions.UploadInstructions;
import org.javaswift.joss.swift.Swift;
import org.junit.Before;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by synhershko on 10/07/2017.
 */
public class SwiftRepositoryPluginTests extends ESIntegTestCase {
    /**
     * Returns a collection of plugins that should be loaded on each node.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(SwiftRepositoryPlugin.class);
    }

    protected Swift swift;

    protected AccountMock account;

    protected ContainerMock container;

    protected StoredObjectMock object;

    protected UploadInstructions instructions;

    @Before
    public void setup() {
        this.swift = new Swift();
        this.account = new AccountMock(swift);
        this.container = new ContainerMock(account, "does-not-exist");
        this.object = new StoredObjectMock(container, "does-not-exist");
        this.instructions = new UploadInstructions(new byte[] { 0x01, 0x02, 0x03 });
    }

    public void testPlugin() {
        container.create();
        assert(true);
    }

    public void testPluginSettings(){
        List<Setting<?>> settings = new SwiftRepositoryPlugin().getSettings();
        assertTrue(settings.stream().anyMatch(s -> s.getKey()== SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING.getKey()));
        assertTrue(settings.stream().anyMatch(s -> s.getKey()== SwiftRepository.Swift.ALLOW_CACHING_SETTING.getKey()));
    }
}
