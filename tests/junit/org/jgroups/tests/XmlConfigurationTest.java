package org.jgroups.tests;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.conf.XmlConfigurator;

import java.io.InputStream;



public class XmlConfigurationTest extends TestCase
{

    protected Log log=LogFactory.getLog(getClass());


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
            if(log.isDebugEnabled()) log.debug(conf.getProtocolStackString());
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
            if(log.isDebugEnabled()) log.debug("XmlConfigurationTest",conf.getProtocolStackString());
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
