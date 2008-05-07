package org.jgroups.conf;
import java.util.Properties;

import org.jgroups.util.Util;

public class PropertyConverters {

    public static class NetworkInterfaceList implements PropertyConverter {

        public Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception {
           return Util.parseInterfaceList(propertyValue);
        }
    }
     
    public static class Default implements PropertyConverter {
        public Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception {
            if(propertyValue == null) throw new NullPointerException("Property value cannot be null");
            if(Boolean.TYPE.equals(propertyFieldType)) {
                return Boolean.parseBoolean(propertyValue);
            }
            else if(Integer.TYPE.equals(propertyFieldType)) {
                return Integer.parseInt(propertyValue);
            }
            else if(Long.TYPE.equals(propertyFieldType)) {
                return Long.parseLong(propertyValue);
            }
            else if(Byte.TYPE.equals(propertyFieldType)) {
                return Byte.parseByte(propertyValue);
            }
            else if(Double.TYPE.equals(propertyFieldType)) {
                return Double.parseDouble(propertyValue);
            }
            else if(Short.TYPE.equals(propertyFieldType)) {
                return Short.parseShort(propertyValue);
            }
            else if(Float.TYPE.equals(propertyFieldType)) {
                return Float.parseFloat(propertyValue);
            }
            return propertyValue;
        }
    }
}
