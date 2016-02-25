package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ECRYPTTest {
    protected JChannel a,b,c,rogue;

    protected void init() {
        a=create("A");
    }


    protected JChannel create(String name) {
        return null;
    }
}
