package org.jgroups.conf;

import java.util.Properties;

import org.jgroups.stack.Protocol ;
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
 * @version $Id: PropertyConverter.java,v 1.5 2009/10/07 20:38:24 rachmatowicz Exp $
 */
public interface PropertyConverter {
    Object convert(Object obj, Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception;

    /**
     * Converts the value to a string. The default is to simply invoke Object.toString(), however, some objects need
     * to be printed specially, e.g. a long array etc.
     * @param value
     * @return
     */
    String toString(Object value);
}
