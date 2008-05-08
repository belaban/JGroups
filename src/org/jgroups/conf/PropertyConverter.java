package org.jgroups.conf;

import java.util.Properties;

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
 * @version $Id: PropertyConverter.java,v 1.2 2008/05/08 09:46:49 vlada Exp $
 */
public interface PropertyConverter {
    Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception;
}
