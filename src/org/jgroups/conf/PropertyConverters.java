package org.jgroups.conf;

import org.jgroups.View;
import org.jgroups.protocols.BasicTCP;
import org.jgroups.protocols.TP;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Groups a set of standard PropertyConverter(s) supplied by JGroups.
 * 
 * <p>
 * Third parties can provide their own converters if such need arises by implementing
 * {@link PropertyConverter} interface and by specifying that converter as converter on a specific
 * Property annotation of a field or a method instance.
 * 
 * @author Vladimir Blagojevic
 * @version $Id: PropertyConverters.java,v 1.13 2009/10/20 14:42:01 belaban Exp $
 */
public class PropertyConverters {

    public static class NetworkInterfaceList implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyValue) throws Exception {
            return Util.parseInterfaceList(propertyValue);
        }

        public String toString(Object value) {
            List<NetworkInterface> list=(List<NetworkInterface>)value;
            return Util.print(list);
        }
    }
    
    public static class FlushInvoker implements PropertyConverter{

		public Object convert(Object obj, Class<?> propertyFieldType, String propertyValue) throws Exception {
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

    public static class InitialHosts implements PropertyConverter{

        public Object convert(Object obj, Class<?> propertyFieldType, String prop_val) throws Exception {
            int port_range = getPortRange((Protocol)obj) ;
            return Util.parseCommaDelimitedHosts(prop_val, port_range);
        }

		public String toString(Object value) {
			return value.getClass().getName();
		}
		
        private static int getPortRange(Protocol protocol) throws Exception {
            Field f = protocol.getClass().getDeclaredField("port_range") ;
            return ((Integer) Configurator.getField(f,protocol)).intValue();
		}
    }
    
    public static class InitialHosts2 implements PropertyConverter {
    	
        public Object convert(Object obj, Class<?> propertyFieldType, String prop_val) throws Exception {
			// port range is 1
            return Util.parseCommaDelimetedHosts2(prop_val, 1);
		}

		public String toString(Object value) {
			return value.getClass().getName();
		}		
    }
    
    public static class BindInterface implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyValue) throws Exception {
       	
        	// get the existing bind address - possibly null
        	InetAddress	old_bind_addr = (InetAddress)Configurator.getValueFromProtocol((Protocol)obj, "bind_addr");
        	
        	// apply a bind interface constraint
            InetAddress new_bind_addr = Util.validateBindAddressFromInterface(old_bind_addr, propertyValue);
            
            if (new_bind_addr != null)
            	setBindAddress((Protocol)obj, new_bind_addr) ;

            // if no bind_interface specified, set it to the empty string to avoid exception
            // from @Property processing
            if (propertyValue != null)
            	return propertyValue ;
            else 
            	return "" ;
        }

        
        private static void setBindAddress(Protocol protocol, InetAddress bind_addr) throws Exception {
            Field f=Util.getField(protocol.getClass(), "bind_addr");
			Configurator.setField(f, protocol, bind_addr) ;
		}
        
        
        // return a String version of the converted value
        public String toString(Object value) {
            return (String) value ;
        }
    }
    
    public static class LongArray implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyValue) throws Exception {
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
        public Object convert(Object obj, Class<?> propertyFieldType, String propertyValue) throws Exception {
            if(propertyValue == null)
                throw new NullPointerException("Property value cannot be null");
            if(Boolean.TYPE.equals(propertyFieldType)) {
                return Boolean.parseBoolean(propertyValue);
            } else if (Integer.TYPE.equals(propertyFieldType)) {
                return Integer.parseInt(propertyValue);
            } else if (Long.TYPE.equals(propertyFieldType)) {
                return Long.parseLong(propertyValue);
            } else if (Byte.TYPE.equals(propertyFieldType)) {
                return Byte.parseByte(propertyValue);
            } else if (Double.TYPE.equals(propertyFieldType)) {
                return Double.parseDouble(propertyValue);
            } else if (Short.TYPE.equals(propertyFieldType)) {
                return Short.parseShort(propertyValue);
            } else if (Float.TYPE.equals(propertyFieldType)) {
                return Float.parseFloat(propertyValue);
            } else if(InetAddress.class.equals(propertyFieldType)) {
            	return InetAddress.getByName(propertyValue) ;
            }
            return propertyValue;
        }

        public String toString(Object value) {
            return value != null? value.toString() : null;
        }
    }
}
