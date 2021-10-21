package org.jgroups.conf;

import org.jgroups.stack.Configurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;

/**
 * Groups a set of standard PropertyConverter(s) supplied by JGroups.
 *
 * <p>
 * Third parties can provide their own converters if such need arises by implementing
 * {@link PropertyConverter} interface and by specifying that converter as converter on a specific
 * Property annotation of a field or a method instance.
 * @author Vladimir Blagojevic
 */
public final class PropertyConverters {

    private PropertyConverters() {
        throw new InstantiationError("Must not instantiate this class");
    }

    public static class NetworkInterfaceList implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope, StackType ip_version) throws Exception {
            return Util.parseInterfaceList(propertyValue);
        }

        public String toString(Object value) {
            List<NetworkInterface> list=(List<NetworkInterface>)value;
            return Util.print(list);
        }
    }

    public static class InitialHosts implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String prop_val, boolean check_scope, StackType ip_version) throws Exception {
            if(prop_val == null)
                return null;
            int port_range=getPortRange((Protocol)obj);
            return Util.parseCommaDelimitedHosts(prop_val, port_range);
        }

        public String toString(Object value) {
            if(value instanceof Collection) {
                StringBuilder sb=new StringBuilder();
                Collection<IpAddress> list=(Collection<IpAddress>)value;
                boolean first=true;
                for(IpAddress addr : list) {
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
            Field f=Util.getField(protocol.getClass(), "port_range");
            return (Integer)Util.getField(f, protocol);
        }
    }

    public static class InitialHosts2 implements PropertyConverter {

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String prop_val, boolean check_scope, StackType ip_version) throws Exception {
            // port range is 0
            return Util.parseCommaDelimitedHosts2(prop_val, 0);
        }

        public String toString(Object value) {
            if(value instanceof Collection) {
                StringBuilder sb=new StringBuilder();
                Collection<InetSocketAddress> list=(Collection<InetSocketAddress>)value;
                boolean first=true;
                for(InetSocketAddress addr : list) {
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

        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue, boolean check_scope, StackType ip_version) throws Exception {

            // get the existing bind address - possibly null
            InetAddress old_bind_addr=Configurator.getValueFromObject((Protocol)obj, "bind_addr");

            // apply a bind interface constraint
            InetAddress new_bind_addr=Util.validateBindAddressFromInterface(old_bind_addr, propertyValue, ip_version);
            if(new_bind_addr != null)
                setBindAddress((Protocol)obj, new_bind_addr);

            // if no bind_interface specified, set it to the empty string to avoid exception from @Property processing
            return propertyValue != null? propertyValue : "";
        }


        private static void setBindAddress(Protocol protocol, InetAddress bind_addr) throws Exception {
            Field f=Util.getField(protocol.getClass(), "bind_addr");
            Util.setField(f, protocol, bind_addr);
        }

        // return a String version of the converted value
        public String toString(Object value) {
            return (String)value;
        }
    }


    public static class Default implements PropertyConverter {


        public Object convert(Object obj, Class<?> propertyFieldType, String propertyName, String propertyValue,
                              boolean check_scope, StackType ip_version) throws Exception {
            if(propertyValue == null)
                throw new NullPointerException("Property value cannot be null");

            if(Boolean.TYPE.equals(propertyFieldType))
                return Boolean.parseBoolean(propertyValue);
            if(Integer.TYPE.equals(propertyFieldType))
                return Util.readBytesInteger(propertyValue);
            if(Long.TYPE.equals(propertyFieldType))
                return Util.readBytesLong(propertyValue);
            if(Byte.TYPE.equals(propertyFieldType))
                return Byte.parseByte(propertyValue);
            if(Double.TYPE.equals(propertyFieldType))
                return Util.readBytesDouble(propertyValue);
            if(Short.TYPE.equals(propertyFieldType))
                return Short.parseShort(propertyValue);
            if(Float.TYPE.equals(propertyFieldType))
                return Float.parseFloat(propertyValue);
            if(InetAddress.class.equals(propertyFieldType)) {
                InetAddress retval=null;
                if(propertyValue.contains(",")) {
                    List<String> addrs=Util.parseCommaDelimitedStrings(propertyValue);
                    for(String addr : addrs) {
                        try {
                            retval=Util.getAddress(addr, ip_version);
                            if(retval != null)
                                break;
                        }
                        catch(Throwable ignored) {
                        }
                    }
                    if(retval == null)
                        throw new IllegalArgumentException(String.format("failed parsing attribute %s with value %s", propertyName, propertyValue));
                }
                else
                    retval=Util.getAddress(propertyValue, ip_version);

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
