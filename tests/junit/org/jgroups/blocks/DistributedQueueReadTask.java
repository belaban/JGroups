// $Id: DistributedQueueReadTask.java,v 1.5 2004/07/05 14:15:02 belaban Exp $
/*
 * Created on Oct 15, 2003
 *
 */
package org.jgroups.blocks;

import org.apache.log4j.Logger;

import java.util.Vector;

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
	
	/**
	 * Build a task which read 'max' elements from queue 'q' and increments 'counter'
	 * when an element is read.
     * @param name
     * @param q
     * @param counter
     * @param max
     * @param timeout
     */
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
			logger.debug("Found item in queue " + name + ':' + contenu);
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


