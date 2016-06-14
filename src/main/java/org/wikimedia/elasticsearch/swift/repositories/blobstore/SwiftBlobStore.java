package org.wikimedia.elasticsearch.swift.repositories.blobstore;

import java.security.PrivilegedAction;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;

/**
 * Our blob store
 */
public class SwiftBlobStore extends AbstractComponent implements BlobStore {
    // How much to buffer our blobs by
    private final int bufferSizeInBytes;

    // Our Swift container. This is important.
    private final Container swift;

    /**
     * Constructor. Sets up the container mostly.
     * @param settings Settings for our repository. Only care about buffer size.
     * @param auth swift account info
     * @param container swift container
     */
    public SwiftBlobStore(Settings settings, final Account auth, final String container) {
        super(settings);
        this.bufferSizeInBytes = (int)settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();
        swift = SwiftPerms.exec(new PrivilegedAction<Container>() {
            @Override
            public Container run() {
                Container swift = auth.getContainer(container);
                if (!swift.exists()) {
                    swift.create();
                    swift.makePublic();
                }
                return swift;
            }
        });
    }

    public boolean moveBlobStorage(final String sourceblob, final String destinationblob){
        return SwiftPerms.exec(new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                StoredObject sourceObject = swift.getObject(sourceblob);
                if(sourceObject.exists()) {
                   StoredObject newObject = swift.getObject(destinationblob);
                   sourceObject.copyObject(swift, newObject);
                   sourceObject.delete();
                   return true;
                }
                return false;
            }
        });
    }

    /**
     * @return the container
     */
    public Container swift() {
        return swift;
    }

    /**
     * @return buffer size
     */
    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    /**
     * Factory for getting blob containers for a path
     * @param path The blob path to search
     */
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new SwiftBlobContainer(path, this);
    }

    /**
     * Delete an arbitrary BlobPath from our store.
     * @param path The blob path to delete
     */
    @Override
    public void delete(final BlobPath path) {
        SwiftPerms.exec(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                String keyPath = path.buildAsString("/");
                if (!keyPath.isEmpty()) {
                    keyPath = keyPath + "/";
                }
                StoredObject obj = swift().getObject(keyPath);
                if (obj.exists()) {
                    obj.delete();
                }
                return null;
            }
        });
    }

    /**
     * Close the store. No-op for us.
     */
    @Override
    public void close() {
    }
}
