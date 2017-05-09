package org.jgroups.conf;

import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Groups a set of standard PropertyConverter(s) supplied by JGroups.
 *
 * <p>
 * Third parties can provide their own converters if such need arises by implementing
 * {@link PropertyConverter} interface and by specifying that converter as converter on a specific
 * Property annotation of a field or a method instance.
 *
 * @author Vladimir Blagojevic
 */
public final class PropertyConverters {

	private PropertyConverters() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    public static class NetworkInterfaceList implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {
            return Util.parseInterfaceList(propertyValue);
        }

        public String toString(Object value) {
            List<NetworkInterface> list=(List<NetworkInterface>)value;
            return Util.print(list);
        }
    }

    public static class FlushInvoker implements PropertyConverter {

		public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {
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

    public static class InitialHosts implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String prop_val, boolean check_scope) throws Exception {
            if(prop_val == null)
                return null;
            int port_range = getPortRange((Protocol)obj) ;
            return Util.parseCommaDelimitedHosts(prop_val, port_range);
        }

		public String toString(Object value) {
            if(value instanceof Collection) {
                StringBuilder sb=new StringBuilder();
                Collection<IpAddress> list=(Collection<IpAddress>)value;
                boolean first=true;
                for(IpAddress addr: list) {
                    if(first)
                        first=false;
                    else
                        sb.append(",");
                    sb.append(addr.getIpAddress().getHostAddress()).append("[").append(addr.getPort()).append("]");
                }
                return sb.toString();
            }
            else
                return value.getClass().getName();
		}

        private static int getPortRange(Protocol protocol) throws Exception {
            Field f = Util.getField(protocol.getClass(), "port_range");
            return (Integer)Util.getField(f, protocol);
		}
    }

    public static class InitialHosts2 implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String prop_val, boolean check_scope) throws Exception {
			// port range is 1
            return Util.parseCommaDelimitedHosts2(prop_val, 1);
		}

		public String toString(Object value) {
            if(value instanceof Collection) {
                StringBuilder sb=new StringBuilder();
                Collection<InetSocketAddress> list=(Collection<InetSocketAddress>)value;
                boolean first=true;
                for(InetSocketAddress addr: list) {
                    if(first)
                        first=false;
                    else
                        sb.append(",");
                    sb.append(addr.getAddress().getHostAddress()).append("[").append(addr.getPort()).append("]");
                }
                return sb.toString();
            }
            else
                return value.getClass().getName();
        }
    }

    public static class BindInterface implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {

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
			Util.setField(f, protocol, bind_addr) ;
		}


        // return a String version of the converted value
        public String toString(Object value) {
            return (String) value ;
        }
    }

    /*public static class IntegerArray implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {
            int[] tmp=Util.parseCommaDelimitedInts(propertyValue);
            if(tmp != null && tmp.length > 0){
                return tmp;
            }
            else
                return null;
        }

        public String toString(Object value) {
            if(value == null)
                return null;
            int[] val=(int[])value;
            StringBuilder sb=new StringBuilder();
            boolean first=true;
            for(int l: val) {
                if(first)
                    first=false;
                else
                    sb.append(",");
                sb.append(l);
            }
            return sb.toString();
        }
    }*/

    public static class LongArray implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {
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

    public static class StringProperties implements PropertyConverter {

        @Override
        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {
            return Util.parseCommaDelimitedProps(propertyValue);
        }

        @Override
        public String toString(Object value) {
            if (value == null)
                return null;
            Map<String, String> v = (Map<String, String>) value;
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for(Entry<String, String> entry : v.entrySet()) {
                if (!first)
                    sb.append(",");
                else
                    first = false;
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
            return sb.toString();
        }

    }


    public static class Default implements PropertyConverter {
        static final String prefix;

        static {
            String tmp="FF0e::";
            try {
                tmp=System.getProperty(Global.IPV6_MCAST_PREFIX);
            }
            catch(Throwable t) {
                tmp="FF0e::";
            }
            prefix=tmp != null? tmp : "FF0e::";
        }

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope) throws Exception {
            if(propertyValue == null)
                throw new NullPointerException("Property value cannot be null");
            if(Boolean.TYPE.equals(propertyFieldType)) {
                return Boolean.parseBoolean(propertyValue);
            } else if (Integer.TYPE.equals(propertyFieldType)) {
                return Util.readBytesInteger(propertyValue);
            } else if (Long.TYPE.equals(propertyFieldType)) {
                return Util.readBytesLong(propertyValue);
            } else if (Byte.TYPE.equals(propertyFieldType)) {
                return Byte.parseByte(propertyValue);
            } else if (Double.TYPE.equals(propertyFieldType)) {
                return Util.readBytesDouble(propertyValue);
            } else if (Short.TYPE.equals(propertyFieldType)) {
                return Short.parseShort(propertyValue);
            } else if (Float.TYPE.equals(propertyFieldType)) {
                return Float.parseFloat(propertyValue);
            } else if(InetAddress.class.equals(propertyFieldType)) {

                InetAddress retval=null;
                if(propertyValue.contains(",")) {
                    List<String> addrs=Util.parseCommaDelimitedStrings(propertyValue);
                    for(String addr: addrs) {
                        try {
                            retval=convertBindAddress(addr);
                            if(retval != null)
                                break;
                        }
                        catch(Throwable t) {
                        }
                    }
                    if(retval == null)
                        throw new IllegalArgumentException(String.format("failed parsing attribute %s with value %s", propertyName, propertyValue));
                }
                else
                    retval=convertBindAddress(propertyValue);

                if(check_scope && retval instanceof Inet6Address && retval.isLinkLocalAddress()) {
                    // check scope
                    Inet6Address addr=(Inet6Address)retval;
                    int scope=addr.getScopeId();
                    if(scope == 0) {
                        // fix scope
                        Inet6Address ret=getScopedInetAddress(addr);
                        if(ret != null)
                            retval=ret;
                    }
                }
                return retval;
            }
            return propertyValue;
        }


        protected static InetAddress convertBindAddress(String value) throws Exception {
            InetAddress retval=null;
            Util.AddressScope addr_scope=null;
            try {
                addr_scope=Util.AddressScope.valueOf(value.toUpperCase());
            }
            catch(Throwable ex) {
            }

            if(addr_scope != null)
                retval=Util.getAddress(addr_scope);
            else {
                if(value.startsWith("match"))
                    retval=Util.getAddressByPatternMatch(value);
                else if(value.startsWith("custom:"))
                    retval=getAddressByCustomCode(value.substring("custom:".length()));
                else
                    retval=InetAddress.getByName(value);
            }

            if(retval instanceof Inet4Address && retval.isMulticastAddress() && Util.getIpStackType() == StackType.IPv6) {
                String tmp=prefix + value;
                retval=InetAddress.getByName(tmp);
                return retval;
            }
            return retval;
        }

        protected static InetAddress getAddressByCustomCode(String value) throws Exception {
            Class<Supplier<InetAddress>> clazz=(Class<Supplier<InetAddress>>)Util.loadClass(value, (ClassLoader)null);
            Supplier<InetAddress> supplier=clazz.newInstance();
            return supplier.get();
        }

        protected static Inet6Address getScopedInetAddress(Inet6Address addr) {
            if(addr == null)
                return null;
            Enumeration<NetworkInterface> en;
            List<InetAddress> retval=new ArrayList<>();

            try {
                en=NetworkInterface.getNetworkInterfaces();
                while(en.hasMoreElements()) {
                    NetworkInterface intf=en.nextElement();
                    Enumeration<InetAddress> addrs=intf.getInetAddresses();
                    while(addrs.hasMoreElements()) {
                        InetAddress address=addrs.nextElement();
                        if(address.isLinkLocalAddress() && address instanceof Inet6Address &&
                                address.equals(addr) && ((Inet6Address)address).getScopeId() != 0) {
                            retval.add(address);
                        }
                    }
                }
                if(retval.size() == 1) {
                    return (Inet6Address)retval.get(0);
                }
                else
                    return null;
            }
            catch(SocketException e) {
                return null;
            }
        }

        public String toString(Object value) {
            if(value instanceof InetAddress)
                return ((InetAddress)value).getHostAddress();
            return value != null? value.toString() : null;
        }
    }
}
