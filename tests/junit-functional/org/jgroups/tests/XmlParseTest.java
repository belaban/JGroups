package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.testng.annotations.Test;

/**
 * Tests XML file ./conf/comment-test.xml. This file contains invalid XML, but the JGroups parser is more lenient and
 * accepts almost all XML, e.g. nested comments, comments within attributes etc.
 * @author Bela Ban
 * @since  5.2.15
 */
@Test(groups= Global.FUNCTIONAL)
public class XmlParseTest {
    public void testXML() throws Exception {
        try(JChannel ch=new JChannel("comment-test.xml").name("X")) {
            ch.connect(getClass().getSimpleName());
            assert ch.isConnected();
        }
    }
}
