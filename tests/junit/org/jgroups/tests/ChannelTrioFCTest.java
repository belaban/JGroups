// $Id: ChannelTrioFCTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelTrioFCTest extends ChannelTrio
{

	/**
     * @param Name_
     */
    public ChannelTrioFCTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/fc.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelTrioFCTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
