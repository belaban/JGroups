// $Id: ChannelDuoFlowControlTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelDuoFlowControlTest extends ChannelDuo
{

	/**
     * @param Name_
     */
    public ChannelDuoFlowControlTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/flow_control.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelDuoFlowControlTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
