package org.jgroups.tests;

import java.io.InputStream;
import junit.framework.TestCase;
import org.jgroups.conf.XmlConfigurator;
import org.jgroups.log.Trace;



public class XmlConfigurationTest extends TestCase
{
    

    public XmlConfigurationTest(String Name_)
    {
        super(Name_);
    }

    protected void setUp()
    {
    }

    protected void tearDown()
    {
    }
    
    public void testBasic()
    {
        try
        {
	    InputStream default_config = XmlConfigurator.class.getClassLoader().getResourceAsStream("default.xml");
	    XmlConfigurator conf = XmlConfigurator.getInstance(default_config);
            Trace.debug("XmlConfigurationTest",conf.getProtocolStackString());
            assertTrue("Successfully parsed a valid XML configuration file.",true);
        }
        catch ( Exception x )
        {
            assertTrue(x.getMessage(),false);
        }
    }

    /* We currently have no inherited property file to test
    public void testInherited()
    {
        try
        {
            XmlConfigurator conf = XmlConfigurator.getInstance(new java.net.URL("http://www.filip.net/jgroups/jgroups-protocol-inherited.xml"));
            Trace.debug("XmlConfigurationTest",conf.getProtocolStackString());
            assertTrue("Successfully parsed a valid XML configuration file that inherits another one.",true);
        }
        catch ( Exception x )
        {
            assertTrue(x.getMessage(),false);
        }
    }
    */

    
    public static void main(String[] args)
    {
        String[] testCaseName = {XmlConfigurationTest.class.getName()};
        junit.swingui.TestRunner.main(testCaseName);
    } //public static void main(String[] args)

} //public class XmlConfigurationTest extends TestCase
