package org.jgroups.conf;

import java.util.Properties;

public interface PropertyConverter {
    Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception;
}
