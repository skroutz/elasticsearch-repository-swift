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

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

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

    public void testPlugin() {
        assert(true);
    }
}
