// $Id: TotalTokenProtocolObserver.java,v 1.3 2004/03/30 06:47:30 belaban Exp $
package org.jgroups.blocks;

import org.apache.log4j.Logger;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolObserver;

import java.util.Vector;

public class TotalTokenProtocolObserver  implements ProtocolObserver
{
	String name;
	static Logger logger = Logger.getLogger(TotalTokenProtocolObserver.class.getName());

    
	public TotalTokenProtocolObserver(JChannel c)
	{
		this.name = c.getLocalAddress().toString();
		
		Vector prots = c.getProtocolStack().getProtocols();
		for (int i = 0; i < prots.size(); i++)
		{
			Protocol prot = (Protocol) prots.elementAt(i);
			if (prot.getName().equals("TOTAL_TOKEN"))
			{
				prot.setObserver(this);
			}
		}
	}

	public void setProtocol(Protocol prot)
	{
	}

	public boolean up(Event evt, int num_evts)
	{
		return true;
	}

	public boolean passUp(Event evt)
	{
		Object obj = null;
		Message msg;

		if (evt.getType() != Event.MSG)
		{
			logger.debug("For channel "+name+" received event:"+evt);
			return true;
		}

		msg = (Message) evt.getArg();
		if (msg.getLength() > 0)
		{
			try
			{
                obj=msg.getObject();
			}
			catch (ClassCastException cast_ex)
			{
				logger.debug("For channel "+name+" received:"+msg);
				return true;
			}
			catch (Exception e)
			{
				logger.error(e);
			}

			logger.debug("For channel "+name+" received:"+obj);
		}
		else
			logger.debug(
				"For channel "+name+" received null msg from "
					+ msg.getSrc()
					+ ", headers are "
					+ msg.printObjectHeaders()
					+ ")");

		return true;
	}

	public boolean down(Event evt, int num_evts)
	{
		Object       obj=null;
		Message      msg;

		if(evt.getType() != Event.MSG) {
			logger.debug("For channel "+name+" sent event:"+evt);
			return true;
		}

		msg=(Message)evt.getArg();
		if(msg.getLength() > 0) {
			try {
                obj=msg.getObject();
			}
			catch(ClassCastException cast_ex) {
				logger.debug("For channel "+name+" sent:"+msg);
				return true;
			}
			catch(Exception e) {
			logger.error(e);
			}
			logger.debug("For channel "+name+" sent:"+obj);

		}
		else
			logger.debug("For channel "+name+" sent null msg to " + msg.getDest() + ", headers are " +
					   msg.printObjectHeaders() + " )");
		return true;
	}
	
	public boolean passDown(Event evt)
	{
		return true;
	}

}
