// $Id: DistributedQueuePutTask.java,v 1.1 2003/10/15 16:15:09 rds13 Exp $
/*
 * Created on Oct 15, 2003
 *
 */
package org.jgroups.blocks;

import java.util.Vector;

import org.jgroups.util.Util;

/**
 * @author Romuald du Song
 */
public class DistributedQueuePutTask implements Runnable
{
    protected DistributedQueue queue;
    protected String name;
    protected boolean finished;
    protected Vector content;
    protected int max;
    protected int delay;

    public DistributedQueuePutTask(String name, DistributedQueue q, int max, int delay)
    {
        this.queue = q;
        this.name = name;
        this.max = max;
        this.delay = delay;
        finished = false;
        content = new Vector();
    }

    public void run()
    {
        for (int i = 0; i < max; i++)
        {
            String item = name + "_" + i;
            queue.add(item);
            content.addElement(item);
            if (delay > 0)
                Util.sleepRandom(delay);
        }
        finished = true;
    }

    /**
     * @return
     */
    public Vector getContent()
    {
        return content;
    }

    public boolean finished()
    {
        return finished;
    }
}
