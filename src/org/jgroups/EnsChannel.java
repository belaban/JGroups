// $Id: EnsChannel.java,v 1.5 2004/07/05 06:00:40 belaban Exp $

package org.jgroups;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.ensemble.*;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;

import java.io.Serializable;
import java.util.Vector;





/**
 * EnsChannel is an implementation of <em>Channel</em> based on 
 * <a href=http://www.cs.cornell.edu/Info/Projects/Ensemble/index.html>Ensemble</a>. It 
 * maps a process group to a channel. Requirements are the presence of an executable called
 * <em>outboard</em> (which is Ensemble) and <em>gossip</em> running.
 */
public class EnsChannel extends Channel implements Hot_Callbacks {
    private Hot_Ensemble        ensemble=null;
    private Thread              ens_thread=null;
    private Hot_GroupContext    channel_id=null;
    private String              channel_name=null;
    private Hot_JoinOps         options=new Hot_JoinOps();
    private Queue               mq=new Queue();
    private Address             my_addr=null;
    private View                my_view=null;
    private final String        default_properties="Gmp:Sync:Heal:Frag:Suspect:Flow:Total";

    private boolean             receive_views=true;
    private boolean             receive_suspects=true;
    private boolean             receive_blocks=false;
    private boolean             receive_local_msgs=true;

    protected Log log=LogFactory.getLog(this.getClass());


    private void checkConnection() throws ChannelNotConnectedException {
	if(channel_id == null)
	    throw new ChannelNotConnectedException();
    }


    private void checkClosed() throws ChannelClosedException {
	if(ensemble == null)
	    throw new ChannelClosedException();
    }


    public EnsChannel() throws ChannelException {
	this(null);
    }


    /**
     Creates a new EnsChannel, which spawns an outboard process and connects to it.
     @param props Ensemble properties (cf. Ensemble reference manual).
     A value of <code>null</code> uses the default properties.
     */
    public EnsChannel(Object props) throws ChannelException {
	String properties=null;

	try {
	    properties=(String)props;
	}
	catch(ClassCastException cce) {
	    throw new ChannelException("EnsChannel(): properties argument must be of type String !");
	}

	options.heartbeat_rate=5000;
	options.transports="UDP";
	options.properties=properties == null? default_properties : properties;	
	options.params="suspect_max_idle=3:int;suspect_sweep=3.000:time";
	options.conf=this;
	options.use_properties=true;
	ensemble=new Hot_Ensemble();
	ens_thread=new Thread(ensemble, "EnsembleThread");
        ens_thread.setDaemon(true);
	ens_thread.start();
    }

    /**
       Creates a new EnsChannel. Instead of spawning a new outboard process, it connects to an
       already running outboard process (on the same machine) using <code>outboard_port</code>.
       This allows multiple EnsChannels to share a copy of outboard. If the port is 0, outboard
       <em>will</em> be spawned. Parameter <code>transport_props</code> defines the type 
       of transport to be used (UDP, ATM, IP MCAST etc).
       @param props Ensemble properties (cf. Ensemble reference manual).
                         A value of <code>null</code> uses the default properties.
       @param transport_props Transport parameters. <code>Null</code> means use default (UDP).
                              Example: <code>"UDP:DEERING"</code> uses IP multicast (gossip is not
			      needed in this case).
       @param outboard_port Port on which the local outboard process is listening. The outboard
                            process has to be started before. Value of 0 means spawn outboard
			    nevertheless.
    */
    public EnsChannel(Object props, String transport_props, int outboard_port) 
	throws ChannelException {

	String properties=null;

	try {
	    properties=(String)props;
	}
	catch(ClassCastException cce) {
	    throw new ChannelException("EnsChannel(): properties argument must be of type String !");
	}

	// channel_name=name;
	options.heartbeat_rate=5000;
	options.transports="UDP";
	if(transport_props != null)
	    options.transports=transport_props;
	// options.group_name=channel_name;
	options.properties=properties == null? default_properties : properties;	
	options.params="suspect_max_idle=3:int;suspect_sweep=3.000:time";
	options.conf=this;
	options.use_properties=true;
	ensemble=outboard_port == 0 ? new Hot_Ensemble() : new Hot_Ensemble(outboard_port);
	ens_thread=new Thread(ensemble, "EnsembleThread");
        ens_thread.setDaemon(true);
	ens_thread.start();
    }
    

    public void connect(String channel_name) throws ChannelClosedException {
	Hot_Error         rc;
	Hot_GroupContext  tmp[]=new Hot_GroupContext[1];

	checkClosed();

	this.channel_name=channel_name;
	options.group_name=channel_name;
	
	if(channel_id != null) {
	    if(log.isErrorEnabled()) log.error("already connected to " + channel_name);
	    return;
	}

	if(ensemble == null || ens_thread == null) {
	    if(log.isErrorEnabled()) log.error("Ensemble has not been started");
	    return;
	}


	rc=ensemble.join(options, tmp);
	if(rc != null) {
	    if(log.isErrorEnabled()) log.error(rc.toString());
	    return;
	}
	channel_id=tmp[0];
	if(channel_listener != null)
	    channel_listener.channelConnected(this);
    }



    public void disconnect() {
	Hot_Error rc;
	if(channel_id == null) {
	    if(log.isErrorEnabled()) log.error("cannot disconnect as channel id is null");
	    return;
	}
	rc=ensemble.leave(channel_id);	
	if(rc != null)
	    if(log.isErrorEnabled()) log.error("rc=" + rc);
	channel_id=null;
	if(channel_listener != null)
	    channel_listener.channelDisconnected(this);
    }



    public void close() {
	if(ensemble != null) {
	    if(ens_thread != null) {
		ensemble.stopEnsThread();
		ens_thread.interrupt();
		ens_thread=null;
	    }
	    try {
		Thread.sleep(500);
	    }
	    catch(Exception e) {
		if(log.isErrorEnabled()) log.error("exception=" + e);
	    }
	    ensemble.destroyOutboard();
	    ensemble=null;
	    try {
		mq.close(false);
	    }
	    catch(Exception e) {
		if(log.isErrorEnabled()) log.error("exception=" + e);
	    }
	    mq.reset();
	    if(channel_listener != null)
		channel_listener.channelClosed(this);
	}
    }


    public boolean isOpen() {
	return channel_id != null;
    }


    public boolean isConnected() {
	return isOpen();
    }






    private void cast(byte[] msg) throws ChannelNotConnectedException, ChannelClosedException {
	Hot_ObjectMessage  m;
	Hot_Error          rc;
	int                tmp[]=new int[1];

	checkConnection();
	checkClosed();

	m=new Hot_ObjectMessage(new Message(null, null, msg));

	rc=ensemble.cast(channel_id, m, tmp);
	if(rc != null)
	    if(log.isErrorEnabled()) log.error("rc=" + rc);
    }


    private void send(Object dest_addr, byte[] msg) throws ChannelNotConnectedException, ChannelClosedException {
	Hot_Endpoint        dest;
	Hot_Error           rc;
	Hot_ObjectMessage   m;
	int                 tmp[]=new int[1];

	checkConnection();
	checkClosed();

	if(dest_addr == null) {
	    if(log.isErrorEnabled()) log.error("destination is null");
	    return;
	}
	dest=(Hot_Endpoint)dest_addr;
	m=new Hot_ObjectMessage(new Message(dest, null, msg));

	rc=ensemble.send(channel_id, dest, m, tmp);
	if(rc != null)
	    if(log.isErrorEnabled()) log.error("rc=" + rc);
    }





    public void send(Message msg) throws ChannelNotConnectedException, ChannelClosedException  {
	Object             dest=msg.getDest();
	Hot_ObjectMessage  m=new Hot_ObjectMessage(msg);
	Hot_Error          rc=null;
	int                tmp[]=new int[1];

	checkConnection();
	checkClosed();
	
	if(dest == null || dest instanceof String || dest instanceof Vector)
	    rc=ensemble.cast(channel_id, m, tmp);
	else if(dest instanceof Hot_Endpoint)
	    rc=ensemble.send(channel_id, (Hot_Endpoint)dest, m, tmp);
	else {
	    if(log.isErrorEnabled()) log.error("dest address is wrong (" + dest + ')');
	    return;
	}

	if(rc != null)
	    if(log.isErrorEnabled()) log.error("rc=" + rc);
    }


    
    public void send(Address dst, Address src, Serializable obj) throws ChannelNotConnectedException, ChannelClosedException  {
        send(new Message(dst, src, obj));
    }
	


    public Object receive(long timeout) throws ChannelNotConnectedException, ChannelClosedException,  TimeoutException {
	Event      evt;

	checkConnection();
	checkClosed();
	    
	if(mq == null)
	    throw new ChannelNotConnectedException();
	    
	try {
	    evt=(timeout <= 0)? (Event)mq.remove() : (Event)mq.remove(timeout);
	    if(evt == null)
		return null; // correct ?

	    switch(evt.getType()) {
	    case Event.MSG:
		return evt.getArg();
	    case Event.VIEW_CHANGE:
		my_view=(View)evt.getArg();
		return my_view;
	    case Event.SUSPECT:
		return new SuspectEvent(evt.getArg());
	    case Event.BLOCK:
		return new BlockEvent();
	    default:
		if(log.isErrorEnabled()) log.error("event is neither message nor view nor block");
		return null;
	    }
	}
	catch(TimeoutException tex) {
	    throw tex;
	}
	catch(QueueClosedException queue_closed) {
	    if(log.isErrorEnabled()) log.error("exception=" + queue_closed);
	    throw new ChannelNotConnectedException();
	}
	catch(Exception e) {
	    if(log.isErrorEnabled()) log.error("exception=" + e);
	    return null;
	}
    }




    public Object peek(long timeout) throws ChannelNotConnectedException, ChannelClosedException,  TimeoutException {
	Event      evt;

	checkConnection();
	checkClosed();
	    
	if(mq == null)
	    throw new ChannelNotConnectedException();
	    
	try {
	    evt=(timeout <= 0)? (Event)mq.peek() : (Event)mq.peek(timeout);
	    if(evt == null)
		return null; // correct ?

	    switch(evt.getType()) {
	    case Event.MSG:
		return evt.getArg();
	    case Event.VIEW_CHANGE:
		return evt.getArg();
	    case Event.SUSPECT:
		return new SuspectEvent(evt.getArg());
	    case Event.BLOCK:
		return new BlockEvent();
	    default:
		if(log.isErrorEnabled()) log.error("event is neither message nor view nor block");
		return null;
	    }
	}
	catch(TimeoutException tex) {
	    throw tex;
	}
	catch(QueueClosedException queue_closed) {
	    if(log.isErrorEnabled()) log.error("exception=" + queue_closed);
	    throw new ChannelNotConnectedException();
	}
	catch(Exception e) {
	    if(log.isErrorEnabled()) log.error("exception=" + e);
	    return null;
	}
    }



    public View     getView()         {return my_view;}

    
    public Address  getLocalAddress() {return my_addr;}


    public String   getChannelName()  {return channel_name;}



    public void    setOpt(int option, Object value) {
	switch(option) {
	case VIEW:
	    if(value instanceof Boolean)
		receive_views=((Boolean)value).booleanValue();
	    else
		if(log.isErrorEnabled()) log.error("(" + option + ", " + value + "): value has " +
			    "to be Boolean");
	    break;
	case SUSPECT:
	    if(value instanceof Boolean)
		receive_suspects=((Boolean)value).booleanValue();
	    else
		if(log.isErrorEnabled()) log.error("(" + option + ", " + value + "): value has " +
			    "to be Boolean");
	    break;
	case BLOCK:
	    if(value instanceof Boolean)
		receive_blocks=((Boolean)value).booleanValue();
	    else
		if(log.isErrorEnabled()) log.error("(" + option + ", " + value + "): value has " +
			    "to be Boolean");
	    if(receive_blocks)
		receive_views=true;
	    break;

	case LOCAL:
	    if(value instanceof Boolean)
		receive_local_msgs=((Boolean)value).booleanValue();
	    else
		if(log.isErrorEnabled()) log.error("(" + option + ", " + value + "): value has " +
			    "to be Boolean");
	    break;
	default:
	    if(log.isErrorEnabled()) log.error("(" + option + ", " + value + "): option not known");
	    break;
	}
    }



    public Object  getOpt(int option) {
	switch(option) {
	case VIEW:
	    return Boolean.valueOf(receive_views);
	case SUSPECT:
	    return Boolean.valueOf(receive_suspects);
	case BLOCK:
	    return Boolean.valueOf(receive_blocks);
	case LOCAL:
	    return Boolean.valueOf(receive_local_msgs);
	default:
	    if(log.isErrorEnabled()) log.error("(" + option + "): option not known");
	    return null;
	}
    }



    public void    blockOk() {
	
    }



    public boolean  getState(Address target, long timeout) {
	return false;
    }


    public boolean  getAllStates(Vector targets, long timeout) {
	return false;
    }


    public void returnState(byte[] state) {

    }






    /* --------------------- Ensemble Callbacks ------------------------- */

    public void receiveCast(Hot_GroupContext gctx, Object env, Hot_Endpoint origin, Hot_Message msg) {
	Hot_ObjectMessage   tmp=new Hot_ObjectMessage(msg);
	Message             m=(Message)tmp.getObject();

	if(m == null) {
	    if(log.isWarnEnabled()) log.warn("received message that is " +
		       "not of type Message. Discarding.");
	    return;
	}

	if(m.getSrc() == null)
	    m.setSrc(origin);

	if(!receive_local_msgs) {  // discard local messages (sent by myself to me)
	    if(my_addr != null && m.getSrc() != null)
		if(my_addr.equals(m.getSrc()))
		    return;
	}
	try {
	    mq.add(new Event(Event.MSG, m));
	}
	catch(Exception e) {
	    if(log.isErrorEnabled()) log.error("exception=" + e);
	}
    }



    public void receiveSend(Hot_GroupContext gctx, Object env, Hot_Endpoint origin, Hot_Message msg) {
	receiveCast(gctx, env, origin, msg);
    }



    public void acceptedView(Hot_GroupContext gctx, Object env, Hot_ViewState viewState) {
	View          v;
	Hot_ViewID    vid=viewState.view_id;
	Address       coord=vid.coord;
	long          id=vid.ltime;
	Vector        members=new Vector();


	if(my_addr == null && viewState.members != null && viewState.nmembers == 1) {
	    my_addr=viewState.members[0];
	    if(log.isInfoEnabled()) log.info("my address is " + my_addr);
	}

	for(int i=0; i < viewState.members.length; i++)
	    members.addElement(viewState.members[i]);

	v=new View(coord, id, members);

	if(my_view == null)
	    my_view=v;

	if(!receive_views)
	    return;

	try {
	    mq.add(new Event(Event.VIEW_CHANGE, v));
	}
	catch(Exception e) {
	    if(log.isErrorEnabled()) log.error("exception=" + e);
	}
    }



    public void heartbeat(Hot_GroupContext gctx, Object env, int rate) {}


    public void block(Hot_GroupContext gctx, Object env) {
	if(!receive_blocks)
	    return;
	try {
	    mq.add(new Event(Event.BLOCK));
	}
	catch(Exception e) {
	    if(log.isErrorEnabled()) log.error("exception=" + e);
	}
    }


    public void exit(Hot_GroupContext gctx, Object env) {
	if(log.isInfoEnabled()) log.info("received EXIT message !");
    }


    /* ------------------------------------------------------------------ */
}
