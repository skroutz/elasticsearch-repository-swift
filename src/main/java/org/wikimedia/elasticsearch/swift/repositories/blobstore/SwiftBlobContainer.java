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

package org.wikimedia.elasticsearch.swift.repositories.blobstore;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.javaswift.joss.exception.CommandException;
import org.javaswift.joss.exception.NotFoundException;
import org.javaswift.joss.model.Directory;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map;
import java.util.List;

/**
 * Swift's implementation of the AbstractBlobContainer
 */
public class SwiftBlobContainer extends AbstractBlobContainer {
    // Our local swift blob store instance
    protected final SwiftBlobStore blobStore;

    // The root path for blobs. Used by buildKey to build full blob names
    protected final String keyPath;

    private final boolean blobExistsCheckAllowed;

    /**
     * Constructor
     * @param path The BlobPath to find blobs in
     * @param blobStore The blob store to use for operations
     */
    protected SwiftBlobContainer(BlobPath path, SwiftBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        String keyPath = path.buildAsString();
        if (!keyPath.isEmpty() && !keyPath.endsWith("/")) {
            keyPath = keyPath + "/";
        }
        this.keyPath = keyPath;
        this.blobExistsCheckAllowed = keyPath.isEmpty() ||
            !blobStore.getSettings().getAsBoolean(SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING.getKey(),
                                        true);
    }

    /**
     * Does a blob exist? Self-explanatory.
     */
    public boolean blobExists(final String blobName) {
        return SwiftPerms.exec(() -> blobStore.swift().getObject(buildKey(blobName)).exists());
    }

    /**
     * Delete a blob. Straightforward.
     * @param blobName A blob to delete
     */
    public void deleteBlob(final String blobName) throws IOException {
        CommandException ex = SwiftPerms.exec(() -> {
            StoredObject object = blobStore.swift().getObject(buildKey(blobName));
            try {
                object.delete();
                return null;
            } catch (CommandException e) {
                return e;
            }
        });

        if (ex != null) {
            throw new NoSuchFileException(blobName, null, "Requested blob was not found " + ex);
        }
    }


    /**
     * Deletes this container and all its contents from the repository.
     *
     * @return delete result
     * @throws IOException on failure
     */
    @Override
    public DeleteResult delete() throws IOException {
        return blobStore.delete(this.path());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
      if (blobNames.isEmpty()) {
        return;
      }

      for(String blobName : blobNames) {
        try {
          deleteBlob(blobName);
        } catch (NoSuchFileException e) {
        } catch (Exception e) {
          throw new IOException("Exception during bulk delete", e);
        }
      }
    }

    /**
     * Get the blobs matching a given prefix
     * @param blobNamePrefix The prefix to look for blobs with
     * @return blobs metadata
     */
    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable final String blobNamePrefix) {
        return SwiftPerms.exec(() -> {
            MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
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
            return blobsBuilder.immutableMap();
        });
    }

    /**
     * Get all the blobs
     */
    @Override
    public Map<String, BlobMetaData> listBlobs() {
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

    /**
     * Fetch a given blob into a BufferedInputStream
     * @param blobName The blob name to read
     * @return a stream
     */
    @Override
    public InputStream readBlob(final String blobName) throws IOException {
        try {
            final InputStream is = SwiftPerms.exec(
                    (PrivilegedAction<InputStream>) () -> new BufferedInputStream(
                            blobStore.swift().getObject(buildKey(blobName)).downloadObjectAsInputStream(),
                            blobStore.bufferSizeInBytes()));

            if (null == is) {
                throw new NoSuchFileException("Blob object [" + blobName + "] not found.");
            }

            return is;
        } catch (NotFoundException e){
            NoSuchFileException e2 = new NoSuchFileException("Blob object [" + blobName + "] not found.");
            e2.initCause(e);
            throw e2;
        }
    }

    @Override
    public void writeBlob(final String blobName, final InputStream in, final long blobSize, boolean failIfAlreadyExists)
                throws IOException {
        if (failIfAlreadyExists && blobExistsCheckAllowed && blobExists(blobName)) {
            throw new FileAlreadyExistsException("blob [" + blobName + "] already exists, cannot overwrite");
        }
        SwiftPerms.exec(() -> {
            blobStore.swift().getObject(buildKey(blobName)).uploadObject(in);
            return null;
        });
    }
}
