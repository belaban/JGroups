package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: PropertyConvertersTest.java,v 1.2 2008/06/02 08:13:09 belaban Exp $
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
        InetAddress addr=InetAddress.getByName("127.0.0.1");
        check(InetAddress.class, "127.0.0.1", addr, conv);
    }

    public static void testNetworkList() throws Exception {
        PropertyConverter conv=new PropertyConverters.NetworkInterfaceList();
        Object tmp=conv.convert(List.class, new Properties(), "eth0,lo");
        Object str=conv.toString(tmp);
        System.out.println("str = " + str);
        assert str.equals("eth0,lo");
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
