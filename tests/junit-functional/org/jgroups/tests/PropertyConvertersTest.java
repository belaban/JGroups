package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: PropertyConvertersTest.java,v 1.5 2008/07/08 14:36:33 vlada Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class PropertyConvertersTest {

    public static void testPrimitiveTypes() throws Exception {
        PropertyConverter conv=new PropertyConverters.Default();
        check(Boolean.TYPE, "true", true, conv);
        check(Integer.TYPE, "322649", 322649, conv);
        check(Long.TYPE, "322649", 322649L, conv);
    }


    public static void testLongArray() throws Exception {
        PropertyConverter conv=new PropertyConverters.LongArray();
        long[] array={1,2,3,4,5};
        checkArray(array.getClass(), "1,2,3,4,5", array, conv);
    }


    public static void testBindAddress() throws Exception {
        PropertyConverter conv=new PropertyConverters.BindAddress();
        InetAddress addr=Util.getBindAddress(new Properties());
        check(InetAddress.class, addr.getHostAddress(),addr, conv);
    }

    /** Cannot really test list of eth0,eth1,lo, because the list differs from host to host
     *
     * @throws Exception
     */
    public static void testNetworkList() throws Exception {
        PropertyConverter conv=new PropertyConverters.NetworkInterfaceList();
        Object tmp=conv.convert(List.class, new Properties(), "lo");
        Object str=conv.toString(tmp);
        System.out.println("str = " + str);
        assert str.equals("lo");
    }


    private static void check(Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(type, new Properties(), prop);
        assert tmp.equals(result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }

    private static void checkArray(Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(type, null, prop);
        assert Arrays.equals((long[])tmp, (long[])result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }
}
