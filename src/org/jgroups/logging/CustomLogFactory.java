package org.jgroups.logging;

/**
 * An extension interface allowing to plug in a custom log provider. Set
 * the {@link LogFactory#setCustomLogFactory(CustomLogFactory)} with your implementation
 * in order for JGroups to use it
 */
public interface CustomLogFactory {

    Log getLog(Class clazz);

    Log getLog(String category);
}
