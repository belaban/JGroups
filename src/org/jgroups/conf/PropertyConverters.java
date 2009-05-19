package org.jgroups.conf;

import java.util.Properties;
import java.util.List;
import java.util.concurrent.Callable;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.NetworkInterface;

import org.jgroups.View;
import org.jgroups.util.Util;

/**
 * Groups a set of standard PropertyConverter(s) supplied by JGroups.
 * 
 * <p>
 * Third parties can provide their own converters if such need arises by
 * implementing {@link PropertyConverter} interface and by specifying that
 * converter as converter on a specific Property annotation of a field or a
 * method instance.
 * 
 * @author Vladimir Blagojevic
 * @version $Id: PropertyConverters.java,v 1.8 2009/05/19 12:59:59 belaban Exp $
 */
public class PropertyConverters {

    public static class NetworkInterfaceList implements PropertyConverter {

        public Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception {
            return Util.parseInterfaceList(propertyValue);
        }

        public String toString(Object value) {
            List<NetworkInterface> list=(List<NetworkInterface>)value;
            StringBuilder sb=new StringBuilder();
            boolean first=true;
            for(NetworkInterface intf: list) {
                if(first)
                    first=false;
                else
                    sb.append(",");
                sb.append(intf.getName());
            }
            return sb.toString();
        }
    }
    
    public static class FlushInvoker implements PropertyConverter{

		public Object convert(Class<?> propertyFieldType, Properties props,
				String propertyValue) throws Exception {
			if (propertyValue == null) {
				return null;
			} else {
				Class<Callable<Boolean>> invoker = (Class<Callable<Boolean>>) Class.forName(propertyValue);
				invoker.getDeclaredConstructor(View.class);
				return invoker;
			}
		}

		public String toString(Object value) {
			return value.getClass().getName();
		}
    	
    }
    
    public static class BindAddress implements PropertyConverter {

        public Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception {
            return Util.getBindAddress(props);
        }

        public String toString(Object value) {
            InetAddress val=(InetAddress)value;
            return val.getHostAddress();
        }
    }
    
    public static class LongArray implements PropertyConverter {

        public Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception {
            long tmp [] = Util.parseCommaDelimitedLongs(propertyValue);
            if(tmp != null && tmp.length > 0){
                return tmp;
            }else{
                // throw new Exception ("Invalid long array specified in " + propertyValue);
                return null;
            }
        }

        public String toString(Object value) {
            if(value == null)
                return null;
            long[] val=(long[])value;
            StringBuilder sb=new StringBuilder();
            boolean first=true;
            for(long l: val) {
                if(first)
                    first=false;
                else
                    sb.append(",");
                sb.append(l);
            }
            return sb.toString();
        }
    }


    public static class Default implements PropertyConverter {
        public Object convert(Class<?> propertyFieldType, Properties props, String propertyValue) throws Exception {
            if(propertyValue == null)
                throw new NullPointerException("Property value cannot be null");
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

        public String toString(Object value) {
            return value != null? value.toString() : null;
        }
    }
}
