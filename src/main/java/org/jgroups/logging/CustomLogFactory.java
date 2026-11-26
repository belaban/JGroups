package org.jgroups.logging;

/**
 * An extension interface allowing to plug in a custom log provider.
 */
public interface CustomLogFactory {

    Log getLog(Class<?> clazz);

    Log getLog(String category);
}
