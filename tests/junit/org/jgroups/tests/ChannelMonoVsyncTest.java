// $Id: ChannelMonoVsyncTest.java,v 1.1 2003/11/24 16:34:20 rds13 Exp $
package org.jgroups.tests;

/**
 * Dummy class to run ChannelMono on default.xml protocol
 * @author rds13
 */
public class ChannelMonoVsyncTest extends ChannelMono
{

	/**
     * @param Name_
     */
    public ChannelMonoVsyncTest(String Name_)
    {
        super(Name_);
		setProtocol("file:conf/vsync.xml");
    }

    public static void main(String[] args)
	{
		String[] testCaseName = { ChannelMonoVsyncTest.class.getName()};
		junit.textui.TestRunner.main(testCaseName);
	}
	
}
