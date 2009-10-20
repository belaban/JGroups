package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: PropertyConvertersTest.java,v 1.9 2009/10/20 14:45:04 belaban Exp $
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
        Object tmp=conv.convert(null, List.class, "lo");
        Object str=conv.toString(tmp);
        System.out.println("str = " + str);
        assert str.equals("lo");
    }


    private static void check(Protocol protocol, Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(protocol, type, prop);
        assert tmp.equals(result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }

    private static void checkArray(Protocol protocol, Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(protocol, type, prop);
        assert Arrays.equals((long[])tmp, (long[])result) : " conversion result: " + tmp + " (" + tmp.getClass() + ")" +
                ", expected result: " + result + " (" + result.getClass() + ")";

        String output=converter.toString(tmp);
        assert output.equals(prop) : "output=" + output + ", prop=" + prop;
    }
}
