// $Id: DistributedQueueReadTask.java,v 1.1 2003/10/15 16:15:10 rds13 Exp $
/*
 * Created on Oct 15, 2003
 *
 */
package org.jgroups.blocks;

import java.util.Vector;
import org.apache.log4j.Logger;

/**
 * @author Romuald du Song
 */
public     class DistributedQueueReadTask implements Runnable
{
	protected DistributedQueue queue;
	protected String name;
	protected boolean finished;
	protected Vector content;
	protected int max;
	protected int timeout;
	protected ICounter counter;

	static Logger logger = Logger.getLogger(DistributedQueueReadTask.class.getName());
	
	public DistributedQueueReadTask(String name, DistributedQueue q, ICounter counter, int max, int timeout)
	{
		this.counter = counter;
		this.queue = q;
		this.name = name;
		this.timeout = timeout;
		finished = false;
		content = new Vector();
		this.max = max;
	}

	public void run()
	{
		while (!finished)
		{
			Object contenu = queue.remove(timeout);
			if (contenu != null)
			{
				counter.increment();
				content.addElement(contenu);
			}
			if (counter.getValue() >= max)
				finished = true;
			logger.debug("Found item in queue " + name + ":" + contenu);
		}
		finished = true;
	}

	public boolean finished()
	{
		return finished;
	}

	/**
	 * @return
	 */
	public Vector getContent()
	{
		return content;
	}

}


