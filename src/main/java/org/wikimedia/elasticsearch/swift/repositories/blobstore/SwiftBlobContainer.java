package org.wikimedia.elasticsearch.swift.repositories.blobstore;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;

import com.google.common.collect.ImmutableMap;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.util.Collection;

/**
 * Swift's implementation of the AbstractBlobContainer
 */
public class SwiftBlobContainer extends AbstractBlobContainer {
    // Our local swift blob store instance
    protected final SwiftBlobStore blobStore;

    // The root path for blobs. Used by buildKey to build full blob names
    protected final String keyPath;

    /**
     * Constructor
     * @param path The BlobPath to find blobs in
     * @param blobStore The blob store to use for operations
     */
    protected SwiftBlobContainer(BlobPath path, SwiftBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }
        this.keyPath = keyPath;
    }

    /**
     * Does a blob exist? Self-explanatory.
     */
    @Override
    public boolean blobExists(final String blobName) {
        return SwiftPerms.exec(new PrivilegedAction<Boolean>() {
                @Override
                public Boolean run() {
                    return blobStore.swift().getObject(buildKey(blobName)).exists();
                }
        });
    }

    /**
     * Delete a blob. Straightforward.
     * @param blobName A blob to delete
     */
    @Override
    public void deleteBlob(final String blobName) throws IOException {
        SwiftPerms.exec(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                StoredObject object = blobStore.swift().getObject(buildKey(blobName));
                if (object.exists()) {
                    object.delete();
                }
                return null;
            }
        });
    }

    /**
     * Get the blobs matching a given prefix
     * @param blobNamePrefix The prefix to look for blobs with
     * @return blobs metadata
     */
    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(@Nullable final String blobNamePrefix) {
        return SwiftPerms.exec(new PrivilegedAction<ImmutableMap<String, BlobMetaData>>() {
            @Override
            public ImmutableMap<String, BlobMetaData> run() {
                ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();
                Collection<DirectoryOrObject> files;
                if (blobNamePrefix != null) {
                    files = blobStore.swift().listDirectory(new Directory(buildKey(blobNamePrefix), '/'));
                } else {
                    files = blobStore.swift().listDirectory(new Directory(keyPath, '/'));
                }
                if (files != null && !files.isEmpty()) {
                    for (DirectoryOrObject object : files) {
                        if (object.isObject()) {
                            String name = object.getName().substring(keyPath.length());
                            blobsBuilder.put(name, new PlainBlobMetaData(name, object.getAsObject().getContentLength()));
                        }
                    }
                }
                return blobsBuilder.build();
            }
        });
    }

    /**
     * Get all the blobs
     */
    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() {
        return listBlobsByPrefix(null);
    }

    /**
     * Build a key for a blob name, based on the keyPath
     * @param blobName The blob name to build a key for
     * @return the key
     */
    protected String buildKey(String blobName) {
        return keyPath + blobName;
    }

    @Override
    public void move(String sourceBlobname, String destinationBlobname) throws IOException {

        String source = buildKey(sourceBlobname);
        String target = buildKey(destinationBlobname);
        if (blobExists(sourceBlobname)) {
            blobStore.moveBlobStorage(source, target);
        }
    }

    /**
     * Fetch a given blob into a BufferedInputStream
     * @param blobName The blob name to read
     * @return a stream
     */
    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        return SwiftPerms.exec(new PrivilegedAction<InputStream>() {
            @Override
            public InputStream run() {
                return new BufferedInputStream(blobStore.swift().getObject(buildKey(blobName)).downloadObjectAsInputStream(),
                        blobStore.bufferSizeInBytes());
            }
        });
    }

    @Override
    public void writeBlob(final String blobName, final BytesReference bytes) throws IOException {
        // need to remove old file if already exist
        deleteBlob(blobName);
        SwiftPerms.exec(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                blobStore.swift().getObject(buildKey(blobName)).uploadObject(new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.length()));
                return null;
            }
        });
    }

    @Override
    public void writeBlob(final String blobName, final InputStream in, final long blobSize) throws IOException {
        // need to remove old file if already exist
        deleteBlob(blobName);
        SwiftPerms.exec(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                blobStore.swift().getObject(buildKey(blobName)).uploadObject(in);
                return null;
            }
        });
    }
}
