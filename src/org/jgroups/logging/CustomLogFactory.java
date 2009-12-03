package org.jgroups.logging;

/**
 * An extension interface allowing to plug in a custom log provider. Set the
 * <code>jgroups.logging.log_factory_class</code> system property with the fully
 * qualified class name of your implementation in order for JGroups to use it
 */
public interface CustomLogFactory {

    Log getLog(Class clazz);

    Log getLog(String category);
}
