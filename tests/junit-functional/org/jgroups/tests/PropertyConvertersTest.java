package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.AbstractProtocol;
import org.testng.annotations.Test;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class PropertyConvertersTest {

    public static void testPrimitiveTypes() throws Exception {
        PropertyConverter conv=new PropertyConverters.Default();
        check(null, Boolean.TYPE, "true", true, conv);
        check(null, Integer.TYPE, "322649", 322649, conv);
        check(null, Long.TYPE, "322649", 322649L, conv);
    }


    public static void testLongArray() throws Exception {
        PropertyConverter conv=new PropertyConverters.LongArray();
        long[] array={1,2,3,4,5};
        checkArray(null, array.getClass(), "1,2,3,4,5", array, conv);
    }




    /** Cannot really test list of eth0,eth1,lo, because the list differs from host to host
     *
     * @throws Exception
     */
    public static void testNetworkList() throws Exception {
        PropertyConverter conv=new PropertyConverters.NetworkInterfaceList();

        String loopback_name=getLoopbackName();
        if(loopback_name == null)
            loopback_name="lo";

        Object tmp;
        try {
            tmp=conv.convert(null, List.class, "bela", loopback_name, false);
        }
        catch(Throwable t) {
            tmp=conv.convert(null, List.class, "bela", "lo0", false); // when running on Mac OS
        }

        Object str=conv.toString(tmp);
        System.out.println("str = " + str);
        assert str.equals(loopback_name) || str.equals("lo0");
    }

    public static void testStringProperties() throws Exception {
        PropertyConverter c = new PropertyConverters.StringProperties();

        String value = "com.sun.security.sasl.digest.realm=MyRealm,qop=true";
        Map<String, String> map = (Map<String, String>) c.convert(null, Map.class, "props", value, false);
        assert map.size() == 2;
        assert map.get("qop").equals("true");
        assert map.get("com.sun.security.sasl.digest.realm").equals("MyRealm");
    }

    private static void check(AbstractProtocol protocol, Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(protocol, type, "bela", prop, false);
        assert tmp.equals(result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }

    private static void checkArray(AbstractProtocol protocol, Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(protocol, type, "bela", prop, false);
        assert Arrays.equals((long[])tmp, (long[])result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }

    private static String getLoopbackName() throws SocketException {
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            intf=(NetworkInterface)en.nextElement();
            if(intf.isLoopback())
                return intf.getName();
        }
        return null;
    }

}
