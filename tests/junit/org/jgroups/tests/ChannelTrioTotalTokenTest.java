// $Id: ChannelTrioTotalTokenTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelTrioTotalTokenTest extends ChannelTrio
{

	/**
     * @param Name_
     */
    public ChannelTrioTotalTokenTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/total-token.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelTrioTotalTokenTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
