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

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.javaswift.joss.exception.CommandException;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;
import org.wikimedia.elasticsearch.swift.SwiftPerms;

import java.util.Collection;

/**
 * Our blob store
 */
public class SwiftBlobStore implements BlobStore {
    // How much to buffer our blobs by
    private final int bufferSizeInBytes;

    // Our Swift container. This is important.
    private final Container swift;

    private final Settings settings;

    /**
     * Constructor. Sets up the container mostly.
     * @param settings Settings for our repository. Only care about buffer size.
     * @param auth swift account info
     * @param container swift container
     */
    public SwiftBlobStore(Settings settings, final Account auth, final String container) {
        this.settings = settings;
        this.bufferSizeInBytes = (int)settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).getBytes();
        swift = SwiftPerms.exec(() -> {
            Container swift = auth.getContainer(container);
            if (!swift.exists()) {
                swift.create();
                swift.makePublic();
            }
            return swift;
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
     * @return deleteResult The delete result
     */
    public DeleteResult delete(final BlobPath path) {
        DeleteResult deleteResult;
        try {
            deleteResult = SwiftPerms.exec(() -> {
                String keyPath = path.buildAsString();
                long bytesDeleted = 0;
                long blobsDeleted = 0;

                if (keyPath.isEmpty() || keyPath.endsWith("/")) {
                    return deleteByPrefix(keyPath.isEmpty() ? swift.listDirectory() : swift.listDirectory(keyPath, '/', "", 100));
                } else {
                    StoredObject obj = swift.getObject(keyPath);
                    if (obj.exists()) {
                        blobsDeleted += 1;
                        bytesDeleted += obj.getContentLength();
                        obj.delete();
                    }
                }
                return new DeleteResult(blobsDeleted, bytesDeleted);
            });
        } catch (CommandException e) {
            if (e.getMessage() != null)
                throw e;
            throw new CommandException(e.toString(), e);
        }

        return deleteResult;
    }

    private DeleteResult deleteByPrefix(Collection<DirectoryOrObject> directoryOrObjects) {
        long blobsDeleted = 0;
        long bytesDeleted = 0;
        DeleteResult dr;

        for (DirectoryOrObject directoryOrObject : directoryOrObjects) {
            if (directoryOrObject.isObject()) {

                StoredObject obj = directoryOrObject.getAsObject();
                bytesDeleted += obj.getContentLength();
                blobsDeleted += 1;
                obj.delete();
            } else {
                dr = deleteByPrefix(swift.listDirectory(directoryOrObject.getAsDirectory()));
                blobsDeleted += dr.blobsDeleted();
                bytesDeleted += dr.bytesDeleted();
            }
        }

        return new DeleteResult(blobsDeleted, bytesDeleted);
    }

    /**
     * Close the store. No-op for us.
     */
    @Override
    public void close() {
    }

    protected Settings getSettings() {
        return settings;
    }
}
