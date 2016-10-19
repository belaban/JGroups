package org.jgroups.jmx;

/**
 * Allows objects to provide references to objects which also expose attributes or operations. These are exposed on
 * the same level as the parent object.
 * @author Bela Ban
 * @since  4.0
 */
public interface AdditionalJmxObjects {
    Object[] getJmxObjects();
}
