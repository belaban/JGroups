// $Id: ChannelTrioStateTransferTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelTrioStateTransferTest extends ChannelTrio
{

	/**
     * @param Name_
     */
    public ChannelTrioStateTransferTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/state_transfer.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelTrioStateTransferTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
