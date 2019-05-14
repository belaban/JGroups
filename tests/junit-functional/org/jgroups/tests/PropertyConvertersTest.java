package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class PropertyConvertersTest {

    public static void testPrimitiveTypes() throws Exception {
        PropertyConverter conv=new PropertyConverters.Default();
        check(null, Boolean.TYPE, "true", true, conv);
        check(null, Integer.TYPE, "322649", 322649, conv);
        check(null, Long.TYPE, "322649", 322649L, conv);
    }


    /** Cannot really test list of eth0,eth1,lo, because the list differs from host to host */
    public static void testNetworkList() throws Exception {
        PropertyConverter conv=new PropertyConverters.NetworkInterfaceList();

        String loopback_name=getLoopbackName();
        if(loopback_name == null)
            loopback_name="lo";

        Object tmp;
        try {
            tmp=conv.convert(null, List.class, "bela", loopback_name, false, Util.getIpStackType());
        }
        catch(Throwable t) {
            tmp=conv.convert(null, List.class, "bela", "lo0", false, Util.getIpStackType()); // when running on Mac OS
        }

        Object str=conv.toString(tmp);
        System.out.println("str = " + str);
        assert str.equals(loopback_name) || str.equals("lo0");
    }


    private static void check(Protocol protocol, Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(protocol, type, "bela", prop, false, Util.getIpStackType());
        assert tmp.equals(result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }

    private static void checkArray(Protocol protocol, Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(protocol, type, "bela", prop, false, Util.getIpStackType());
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
