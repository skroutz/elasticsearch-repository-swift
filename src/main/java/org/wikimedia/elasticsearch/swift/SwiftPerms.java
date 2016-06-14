package org.wikimedia.elasticsearch.swift;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.elasticsearch.SpecialPermission;

/**
 * FIXME: remove this boiler plate code if
 * https://github.com/javaswift/joss/pull/106 is merged
 */
public class SwiftPerms {
    public static <T> T exec(final PrivilegedAction<T> callable) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
            return AccessController.<T>doPrivileged(callable);
        }
        return callable.run();
    }
}
