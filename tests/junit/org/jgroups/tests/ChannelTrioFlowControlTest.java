// $Id: ChannelTrioFlowControlTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelTrioFlowControlTest extends ChannelTrio
{

	/**
     * @param Name_
     */
    public ChannelTrioFlowControlTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/flow_control.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelTrioFlowControlTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
