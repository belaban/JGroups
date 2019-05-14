package org.jgroups.conf;

import org.jgroups.annotations.Property;
import org.jgroups.util.StackType;

/**
 * Represents a property converter that takes an input from corresponding field
 * in JGroups properties file as a String and converts it to a matching Java
 * type.
 * 
 * @see PropertyConverters
 * @see Property
 * 
 * @author Vladimir Blagojevic
 */
public interface PropertyConverter {
    Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue,
                   boolean check_scope, StackType ip_version) throws Exception;

    /**
     * Converts the value to a string. The default is to simply invoke Object.toString(), however, some objects need
     * to be printed specially, e.g. a long array etc.
     * @param value
     * @return
     */
    String toString(Object value);
}
