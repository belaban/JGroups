package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.JChannel;
import org.jgroups.blocks.DistributedHashtable;

import java.net.URL;

/**
 *
 * @author Bela Ban belaban@yahoo.com
 * @version $Revision: 1.1 $
 **/
public class DistributedHashtableUnitTest extends TestCase {

    private static int testCount = 1;

    private static final String props="c:\\udp.xml";

    private DistributedHashtable map1;
    private DistributedHashtable map2;

    public DistributedHashtableUnitTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
        System.out.println("#### Setup Test " + testCount);

        JChannel c1=new JChannel(props);
        this.map1=new DistributedHashtable(c1, false, 5000);
        c1.connect("demo");
        this.map1.start(5000);

        JChannel c2=new JChannel(props);
        this.map2=new DistributedHashtable(c2, false, 5000);
        c2.connect("demo");
        this.map2.start(5000);
    }

    protected void tearDown() throws Exception {
        this.map1.stop();
        this.map2.stop();
        System.out.println("#### TearDown Test " + testCount + "\n\n");
        testCount++;
        super.tearDown();
    }

    public void testClear() {
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());

        this.map1.put("key", "value");
        assertFalse(this.map1.isEmpty());
        assertFalse(this.map2.isEmpty());

        this.map1.clear();
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());
        /*
           this.map2.put("key", "value");
           assertFalse(this.map1.isEmpty());
           assertFalse(this.map2.isEmpty());

           this.map2.clear();
           assertTrue(this.map1.isEmpty());
           assertTrue(this.map2.isEmpty());
            */
    }


}
