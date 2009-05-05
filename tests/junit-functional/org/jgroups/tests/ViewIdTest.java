
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.ViewId;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;

/**
 * Author: Bela Ban
 * Version: $Id: ViewIdTest.java,v 1.3 2009/05/05 15:13:14 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class ViewIdTest {
    private ViewId v1, v2, v3;

    @BeforeClass
    public void setUp() throws UnknownHostException {
        v1=new ViewId(Util.createRandomAddress(), 22);
        v2=new ViewId(Util.createRandomAddress(), 21);
        v3=(ViewId)v1.clone();
    }


    public void test0() {
        assert v1.equals(v2) == false;
    }

    public void test1() {
        assert v1.equals(v3) : "v1 and v3 should be the same view";
    }


    public void test2() {
        ViewId v4=(ViewId)v1.clone();
        assert v1.equals(v4);
    }


    public void test3() {
        assert v1.compareTo(v3) == 0;
    }


    public void test4() {
        assert v1.compareTo(v2) > 0;
    }


    public void test5() {
        assert v2.compareTo(v1) < 0;
    }

   
}


