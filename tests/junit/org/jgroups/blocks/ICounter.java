// $Id: ICounter.java,v 1.1 2003/10/15 16:15:10 rds13 Exp $
/*
 * Created on Oct 15, 2003
 *
 */
package org.jgroups.blocks;

/**
 * @author Romuald du Song
 */
public interface ICounter
{
	public int increment();
	public int getValue();
}
