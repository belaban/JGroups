package org.jgroups.tests;


import org.jgroups.JChannel;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Tests whether UNICAST or NAKACK prevent delivery of duplicate messages. JGroups guarantees that a message is
 * delivered once and only once. The test inserts DUPL below both UNICAST and NAKACK and makes it duplicate (1)
 * unicast, (2) multicast, (3) regular and (4) OOB messages. The receiver(s) then check for the presence of duplicate
 * messages. 
 * @author Bela Ban
 * @version $Id: DuplicateTest.java,v 1.1 2008/06/06 08:26:09 belaban Exp $
 */
public class DuplicateTest extends ChannelTestBase {
    JChannel c1, c2, c3;


    @BeforeMethod
    public void setUp() throws Exception {
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }



    

}