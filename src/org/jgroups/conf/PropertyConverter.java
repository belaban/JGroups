package org.jgroups.conf;

import org.jgroups.annotations.Property;

/**
 * Represents a property converter that takes an input from corresponding field
 * in JGroups properties file as a String and converts it to a matching Java
 * type.
 * 
 * @see PropertyConverters
 * @see Property
 * 
 * @author Vladimir Blagojevic
 * @version $Id: PropertyConverter.java,v 1.7 2009/10/22 13:50:59 belaban Exp $
 */
public interface PropertyConverter {
    Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception;

    /**
     * Converts the value to a string. The default is to simply invoke Object.toString(), however, some objects need
     * to be printed specially, e.g. a long array etc.
     * @param value
     * @return
     */
    String toString(Object value);
}
