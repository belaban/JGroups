// $Id: ChannelTrioDefaultMinimalThreadsTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelTrioDefaultMinimalThreadsTest extends ChannelTrio
{

	/**
     * @param Name_
     */
    public ChannelTrioDefaultMinimalThreadsTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/default-minimalthreads.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelTrioDefaultMinimalThreadsTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
