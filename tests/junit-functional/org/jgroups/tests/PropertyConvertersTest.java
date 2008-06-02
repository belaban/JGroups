package org.jgroups.tests;

import org.testng.annotations.Test;
import org.jgroups.Global;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;

import java.util.Arrays;

/**
 * @author Bela Ban
 * @version $Id: PropertyConvertersTest.java,v 1.1 2008/06/02 08:01:48 belaban Exp $
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


    public static void testBindAddress() {
        PropertyConverter conv=new PropertyConverters.BindAddress();

    }

    private static void check(Class<?> type, String prop, Object result, PropertyConverter converter) throws Exception {
        Object tmp=converter.convert(type, null, prop);
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
