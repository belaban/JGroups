package org.jgroups.tests;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.XmlConfigurator;
import org.jgroups.util.Util;

import java.io.InputStream;


public class XmlConfigurationTest extends TestCase {

    protected Log log=LogFactory.getLog(getClass());


    public XmlConfigurationTest(String Name_) {
        super(Name_);
    }


    public void testBasic() {
        try {
            InputStream default_config=Util.getResourceAsStream("udp.xml", this.getClass());
            XmlConfigurator conf=XmlConfigurator.getInstance(default_config);
            if(log.isDebugEnabled()) log.debug(conf.getProtocolStackString());
            assertTrue("Successfully parsed a valid XML configuration file.", true);
        }
        catch(Exception x) {
            fail(x.getMessage());
        }
    }


    
    public static void main(String[] args) {
        String[] testCaseName={XmlConfigurationTest.class.getName()};
        junit.swingui.TestRunner.main(testCaseName);
    }

}
