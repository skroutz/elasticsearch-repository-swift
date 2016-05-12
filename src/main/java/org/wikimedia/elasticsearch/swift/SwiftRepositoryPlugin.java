package org.wikimedia.elasticsearch.swift;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.repositories.SwiftService;

import java.util.Collection;
import java.util.Collections;

/**
 * Our base plugin stuff.
 */
public class SwiftRepositoryPlugin extends Plugin  {
    // Elasticsearch settings
    private final Settings settings;

    /**
     * Constructor. Sets settings to settings.
     * @param settings Our settings
     */
    public SwiftRepositoryPlugin(Settings settings) {
        this.settings = settings;
    }

    /**
     * Plugin name, duh.
     */
    @Override
    public String name() {
        return "swift-repository";
    }

    /**
     * A description. Also duh.
     */
    @Override
    public String description() {
        return "Swift repository plugin";
    }

    /**
     * Register our services, if needed.
     */

    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        return Collections.<Class<? extends LifecycleComponent>>singleton(SwiftService.class);
    }

    /**
     * Load our repository module into the list, if enabled
     * @param repositoriesModule The repositories module to register ourselves with
     */
    public void onModule(RepositoriesModule repositoriesModule) {
        if (settings.getAsBoolean("swift.repository.enabled", true)) {
            repositoriesModule.registerRepository(SwiftRepository.TYPE, SwiftRepository.class, BlobStoreIndexShardRepository.class);
        }
    }
}
