// $Id: Hot_Ensemble.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Hot_Ensemble extends java.lang.Object implements java.lang.Runnable {
    public final int HOT_ENS_MSG_SEND_UNSPECIFIED_VIEW = 0;
    public final int HOT_ENS_MSG_SEND_NEXT_VIEW = 1;
    public final int HOT_ENS_MSG_SEND_CURRENT_VIEW = 2;


    private Hot_Mutex           WriteMutex;
    private Hot_Mutex           CriticalMutex;
    private boolean             outboardValid=false;
    private Hot_IO_Controller   hioc;
    private Process             ensOutboardProcess=null;
    private Socket              ensOutboardSocket=null;
    private static boolean      debug;
    private boolean             running=true;


	
	
    /**
    Destroys the associated outboard process. All instances of Hot_Ensemble
    (in an instance of the VM) use the same outboard process (currently). 
    So, if you destroy the outboard process for one instance, it gets
    destroyed for all instances. A future version of Ensemble/Java will
    change this restriction.
    */
    public void destroyOutboard() {
	if (ensOutboardProcess != null) {
	    ensOutboardProcess.destroy();
            ensOutboardProcess = null;
            outboardValid = false;
	}

// 	if(ensOutboardSocket != null) {
// 	    try {
// 		ensOutboardSocket.close();
// 	    }
// 	    catch(Exception e) {
// 		System.err.println(e);
// 	    }
// 	    ensOutboardSocket=null;
// 	}

    }


	
    /**
    Constructs a Hot_Ensemble object starting the Ensemble Outboard process
    on a random port between 5000 and 8000
    */
    public Hot_Ensemble() {
	int port = 0;
	ServerSocket s = null;
	while (port == 0) {
            port = (int)((Math.random() * 3000) + 5000);
	    try {
		s = new ServerSocket(port);
	    } catch (Exception e) {
		System.out.println("cant use port: " + port);
		port = 0;
            }
	}
	try {
            s.close();
	} catch (Exception e) {
	
	}
	init(port);
    }




    /**
    Constructs a Hot_Ensemble object starting the Ensemble Outboard 
    process on the specified port. If you use this version of the
    constructor, you are expected to make sure the port is not already in 
    use. You can check for successful startup by invoking isOutboardStarted();
    */
    public Hot_Ensemble(int port) {
	try {
	    ensOutboardSocket = new Socket(InetAddress.getLocalHost(), port);
	    hioc = new Hot_IO_Controller(new BufferedInputStream
					 (ensOutboardSocket.getInputStream()),
					 ensOutboardSocket.getOutputStream());	    
	    outboardValid = true;
	    System.out.println("outboard: outboard found on port " + port);
	    initCritical();
	    initWriteCritical();
	}
	catch(Exception e) {
	    System.err.println(e);
	    System.exit(-1);
	}
    }


	

    /**
    Sets up child process, creates the new IO Controller from this child process
    */
    private void init(int port) {
	String outboard = "outboard -tcp_channel -tcp_port " +  Integer.toString(port);

	if (outboardValid != true) {
            try {
		ensOutboardProcess = Runtime.getRuntime().exec(outboard);

		// Added by TC - wait for forked process to start up.
		try {
		    System.out.println("Waiting for the outboard process to start");
		    Thread.sleep(2500);
		}
		catch(InterruptedException e) {
		    System.out.println(e);
		    System.out.println("Sleep");
		}
		// End Added by TC.


		ensOutboardSocket = new Socket(InetAddress.getLocalHost(),
						port);
		hioc = new Hot_IO_Controller(new BufferedInputStream
						 (ensOutboardSocket.getInputStream()),
						 ensOutboardSocket.getOutputStream());

		outboardValid = true;
		System.out.println("outboard: outboard started on port " + port);
            } catch (SecurityException e) {
		System.out.println(e);
		System.out.println("Security exception, can't run inside web browser.");
		System.exit(2);
            } catch (IOException e) {
		System.err.println("Hot_Ensemble.init(" + port + "): outboard could not be started !");
		System.exit(3);
            }

            // do the hot_ens_init stuff  (nothing here yet because there isn't 
	    // really anything useful to us in there)
	    initCritical();
	    initWriteCritical();
        }
	debug = false;
    }



    public void stopEnsThread() {
	running=false;
    }

	
    /**
    Set whether or not to display lots of debug information.  Default: false
    */
    public void setDebug(boolean b) {
	debug = b;
    }


    /**
    Join the Ensemble group specified in the Hot_JoinOps structure
    */
    public Hot_Error join(Hot_JoinOps jops, Hot_GroupContext[] gctx) {
	if (!outboardValid)
            return new Hot_Error(55,"Outboard process is not valid!");
	trace("Hot_Ensemble::join begin");
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	Hot_GroupContext gc = null;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
	 	gc = Hot_GroupContext.alloc();
		gc.joining = true;
		gc.leaving = false;
		gc.conf = jops.conf;
		gc.env = jops.env;
		gctx[0] = gc;
	    }
            hioc.write_groupID(cs,gc.id);
	    hioc.write_dnType(cs,hioc.DN_JOIN);
	    hioc.write_uint(cs,jops.heartbeat_rate);
	    hioc.write_string(cs,jops.transports);
	    hioc.write_string(cs,jops.protocol);
	    hioc.write_string(cs,jops.group_name);
	    hioc.write_string(cs,jops.properties);
	    hioc.write_bool(cs,jops.use_properties);
	    hioc.write_bool(cs,jops.groupd);
	    hioc.write_string(cs,jops.params);
	    hioc.write_bool(cs,jops.client);
	    hioc.write_bool(cs,jops.debug);
	    hioc.write_checksum(cs[0]);
	    err = hioc.check_write_errors();
	}
	trace("Hot_Ensemble::join end");
	return err;
    }

	
    /**
    Leave the Ensemble group specified in the Hot_GroupContext
    */
    public Hot_Error leave(Hot_GroupContext gc) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
            synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("hot_ens_leave: this member is already leaving");
		}
		gc.leaving = true;
            }
	    // write the downcall.
   	    hioc.write_groupID(cs,gc.id);
	    hioc.write_dnType(cs,hioc.DN_LEAVE);
	    hioc.write_checksum(cs[0]);
	    err = hioc.check_write_errors();
	}
	return err;
    }

	
    /**
    Broadcast a Hot_Message to the group specified in the Hot_GroupContext
    */
    public Hot_Error cast(Hot_GroupContext gc, Hot_Message orig_msg, int[] send_view) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");



	/* ------------------------------  BAD HACK !! --------------------------------

	   Using write_buffer instead of write_msg seems to cause the UDP layer to drop
	   'uneven' messages (uneven number of bytes). Therefore, if the bytes would be
	   uneven, I just copy the message and add some 0 bytes to the end. This should
	   not cause any harm ! (?)

	   --------------------------------------------------------------------------- */
			  

	Hot_Message msg=new Hot_Message();

	byte m[]=orig_msg.getBytes();
	int  len=m.length;
	int  pad=4 - (len % 4);

	if(pad > 0 && pad < 4) {
	    byte new_array[]=new byte[len + pad];
	    System.arraycopy(m, 0, new_array, 0, len);
	    msg.setBytes(new_array);
	}
	else
	    msg.setBytes(orig_msg.getBytes());
	

	trace("Hot_Ensemble::cast begin");
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("hot_ens_leave: member is leaving");
		}
		if (send_view != null) {
		    send_view[0] = gc.group_blocked ? 
			HOT_ENS_MSG_SEND_NEXT_VIEW : HOT_ENS_MSG_SEND_CURRENT_VIEW;
		}
	    }
  	    // write the downcall
	    hioc.write_groupID(cs,gc.id);
	    hioc.write_dnType(cs,hioc.DN_CAST);

	    // hioc.write_msg(cs,msg);
	    hioc.write_actual_buffer(cs, msg.getBytes());	    

	    hioc.write_checksum(cs[0]);
	    err = hioc.check_write_errors();
	}
	trace("Hot_Ensemble::cast end");



// 	switch(send_view[0]) {
// 	case 0:
// 	    System.out.println(" [UNSPEC] ");
// 	    break;
// 	case 1:
// 	    System.out.println(" [NEXT] ");
// 	    break;
// 	case 2:
// 	    System.out.println(" [CURR] ");
// 	    break;
// 	default:
// 	    System.out.println(" ["+send_view[0]+"] ");
// 	    break;
// 	}

	return err;
    }


    /**
    Send a Hot_Message to member specified by the Hot_Endpoint in the group 
    specified by the Hot_GroupContext
    */
    public Hot_Error send(Hot_GroupContext gc, Hot_Endpoint dest, 
			  Hot_Message orig_msg, int[] send_view) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");

	Hot_Message msg=new Hot_Message();

	byte m[]=orig_msg.getBytes();
	int  len=m.length;
	int  pad=4 - (len % 4);

	if(pad > 0 && pad < 4) {
	    byte new_array[]=new byte[len + pad];
	    System.arraycopy(m, 0, new_array, 0, len);
	    msg.setBytes(new_array);
	}
	else
	    msg.setBytes(orig_msg.getBytes());	

	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("hot_ens_Send: member is leaving");
		}
		if (send_view != null) {
		    send_view[0] = gc.group_blocked ? 
			HOT_ENS_MSG_SEND_NEXT_VIEW : HOT_ENS_MSG_SEND_CURRENT_VIEW;
		}
	    }
	    // write the downcall
	    hioc.write_groupID(cs,gc.id);
	    hioc.write_dnType(cs,hioc.DN_SEND);
	    hioc.write_endpID(cs,dest);


	    // hioc.write_msg(cs,msg);

	    hioc.write_actual_buffer(cs, msg.getBytes());

	    hioc.write_checksum(cs[0]);
	    err = hioc.check_write_errors();
	}
	return err;
    }


    /**
    NOT SUPPORTED CURRENTLY IN THE ML
    */
    public Hot_Error suspect(Hot_GroupContext gc, Hot_Endpoint[] suspects) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("hot_ens_Suspect: member is leaving");
		}
	    }
            // write the downcall
            hioc.write_groupID(cs,gc.id);
            hioc.write_dnType(cs,hioc.DN_SUSPECT);
            hioc.write_endpList(cs,suspects);
            hioc.write_checksum(cs[0]);
            err = hioc.check_write_errors();
        }
	return err;
    }


    /**
    Change the protocol used by the group specified by the Hot_GroupContext 
    to the protocol specified by the String
    */
    public Hot_Error changeProtocol(Hot_GroupContext gc, String protocol) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");
	if (protocol == null) {
 	    panic("changeProtocol: don't send me null garbage!!");
	}
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("hot_ens_ChangeProtocol: member is leaving");
		}
	    }
            // write the downcall
            hioc.write_groupID(cs,gc.id);
            hioc.write_dnType(cs,hioc.DN_PROTOCOL);
            hioc.write_string(cs,protocol);
            hioc.write_checksum(cs[0]);
            err = hioc.check_write_errors();
	}
	return err;
    }


    /**
    Change the properties of the group specified by the Hot_GroupContext 
    to the properties specified by the String
    */
    public Hot_Error changeProperties(Hot_GroupContext gc, String properties) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");
	if (properties == null) {
	    panic("changeProperties: don't send me null garbage!!");
	}
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("changeProperties: member is leaving");
		}
	    }
            // write the downcall
            hioc.write_groupID(cs,gc.id);
            hioc.write_dnType(cs,hioc.DN_PROPERTIES);
            hioc.write_string(cs,properties);
            hioc.write_checksum(cs[0]);
            err = hioc.check_write_errors();
	}
	return err;
    }


    /**
    Request a new view in the group specified by the Hot_GroupContext
    */
    public Hot_Error requestNewView(Hot_GroupContext gc) {
	if (!outboardValid)
	    return new Hot_Error(55,"Outboard process is not valid!");
	Hot_Error err = null;
	int[] cs = new int[1];
	cs[0] = 0;
	synchronized(WriteMutex) {
	    synchronized(CriticalMutex) {
		if (gc.leaving) {
		    panic("requestNewView: member is leaving");
		}
	    }
            // write the downcall
            hioc.write_groupID(cs,gc.id);
            hioc.write_dnType(cs,hioc.DN_PROMPT);
            hioc.write_checksum(cs[0]);
            err = hioc.check_write_errors();
	}
	return err;
    }


    /** 
    Receive new view callback and print out info
    */
    void cb_View(int groupID, int[] cs) {
	Hot_ViewState hvs = new Hot_ViewState();
	trace("Hot_Ensemble: VIEW");
	String[] pString = new String[1];

	hioc.read_string(cs,pString);
	hvs.version = pString[0];
	trace("\t version: " + hvs.version);
	
	hioc.read_string(cs,pString);
	hvs.group_name = pString[0];
	trace("\t group_name: " + hvs.group_name);

	int[] pInt = new int[1];
	hvs.members = hioc.read_endpList(cs,pInt);
	
	hvs.nmembers = pInt[0];
	trace("\t nmembers: " + hvs.nmembers);

	hioc.read_uint(cs,pInt);
	hvs.rank = pInt[0];
	trace("\t rank: " + hvs.rank);

	hioc.read_string(cs,pString);
	hvs.protocol = pString[0];
	trace("\t protocol: " + hvs.protocol);

	boolean[] pBool = new boolean[1];
	hioc.read_bool(cs,pBool);
	hvs.groupd = pBool[0];
	trace("\t groupd: " + hvs.groupd);
	
	hvs.view_id = new Hot_ViewID();
	hioc.read_uint(cs,pInt);
	hvs.view_id.ltime = pInt[0];
	trace("\t view_id.ltime: " + hvs.view_id.ltime);
	
	hvs.view_id.coord = new Hot_Endpoint();
	hioc.read_endpID(cs,hvs.view_id.coord);
	trace("\t view_id.coord: " + hvs.view_id.coord);
	
	hioc.read_string(cs,pString);
	hvs.params = pString[0];
	trace("\t params: " + hvs.params);
	
	hioc.read_bool(cs,pBool);
	hvs.xfer_view = pBool[0];
	trace("\t xfer_view: " + hvs.xfer_view);
	
	hioc.read_bool(cs,pBool);
	hvs.primary = pBool[0];
	//hvs.clients = new Hot_BoolList();
	//hioc.read_boolList(cs,hvs.clients);
	hvs.clients = hioc.read_boolList(cs);
	hioc.read_checksum(cs[0]);

	if (hioc.check_read_errors() != null) {
	    panic("HOT: read failed inside cb_View");
	}

	Hot_GroupContext gc = null;
	Object env = null;
	Hot_Callbacks hcb = null;
	
	synchronized(CriticalMutex) {
	    gc = Hot_GroupContext.lookup(groupID);
	    env = gc.env;
	    hcb = gc.conf;
	    gc.group_blocked = false;
	}
	hcb.acceptedView(gc,env,hvs);
	synchronized(CriticalMutex) {
	    // HOT C does some memory cleanup here... we don't have to.
	    gc.joining = false;
	}
	trace("Hot_Ensemble: END VIEW");
    }


    /**
    Multicast msg receive callback
    */
    void cb_Cast(int groupID, int[] cs) {
	Hot_Endpoint hep = new Hot_Endpoint();
	
	hioc.read_endpID(cs,hep);


	Hot_Message hmsg = new Hot_Message();


	// hioc.read_msg(cs,hmsg);

	hioc.read_buffer(cs, hmsg);

	hioc.read_checksum(cs[0]);

	if (hioc.check_read_errors() != null) {
	    panic("HOT: read failed in cb_Cast");
	}

	Hot_GroupContext gc = null;
	Object env = null;
	Hot_Callbacks hcb = null;
	synchronized(CriticalMutex) {
	    gc = Hot_GroupContext.lookup(groupID);
	    env = gc.env;
	    hcb = gc.conf;
	}
	hcb.receiveCast(gc,env,hep,hmsg);
    }


    /**
    Point-to-point msg receive callback
    */
    void cb_Send(int groupID, int[] cs) {
	Hot_Endpoint hep = new Hot_Endpoint();

	hioc.read_endpID(cs,hep);

	Hot_Message hmsg = new Hot_Message();


	// hioc.read_msg(cs,hmsg);

	hioc.read_buffer(cs, hmsg);

	hioc.read_checksum(cs[0]);

	if (hioc.check_read_errors() != null) {
	    panic("HOT: read failed inside cb_Send");
	}

	Hot_GroupContext gc = null;
	Object env = null;
	Hot_Callbacks hcb = null;

	synchronized(CriticalMutex) {
	    gc = Hot_GroupContext.lookup(groupID);
	    env = gc.env;
	    hcb = gc.conf;
	}
	hcb.receiveSend(gc,env,hep,hmsg);
    }


    /**
    Heartbeat receive callback
    */
    void cb_Heartbeat(int groupID, int[] cs) {
	int[] pInt = new int[1];
	pInt[0]=0;
	hioc.read_uint(cs,pInt);

	hioc.read_checksum(cs[0]);

	if (hioc.check_read_errors() != null) {
	    panic("HOT: read failed inside cb_Heartbeat");
	}

	Hot_GroupContext gc = null;
	Object env = null;
	Hot_Callbacks hcb = null;

	synchronized(CriticalMutex) {
	    gc = Hot_GroupContext.lookup(groupID);
            env = gc.env;
            hcb = gc.conf;
	}
	hcb.heartbeat(gc,env,pInt[0]);
    }


    /** 
    Block msg receive callback
    */
    void cb_Block(int groupID, int[] cs) {
	hioc.read_checksum(cs[0]);

	if (hioc.check_read_errors() != null) {
	    panic("HOT: read failed inside cb_Block");
	}

	Hot_GroupContext gc = null;
	Object env = null;
	Hot_Callbacks hcb = null;

	synchronized(CriticalMutex) {
	    gc = Hot_GroupContext.lookup(groupID);
	    env = gc.env;
            hcb = gc.conf;
            gc.group_blocked = true;
	}
	hcb.block(gc,env);

	// write the block_ok downcall
	synchronized(WriteMutex) {
            hioc.write_groupID(cs,gc.id);
            hioc.write_dnType(cs, hioc.DN_BLOCK_ON);
            hioc.write_checksum(cs[0]);
            if (hioc.check_write_errors() != null) {}
	}
    }


    /**
    Exit callback 
    */
    void cb_Exit(int groupID, int[] cs) {
	hioc.read_checksum(cs[0]);

	if (hioc.check_read_errors() != null) {
	    panic("HOT: read failed inside cb_Exit");
	}

	Hot_GroupContext gc = null;
	Object env = null;
	Hot_Callbacks hcb = null;

	synchronized(CriticalMutex) {
	    gc = Hot_GroupContext.lookup(groupID);
	    if (!gc.leaving) {
		panic("hot_ens_Exit_cbd: mbr state is not leaving");
            }
	    env = gc.env;
	    hcb = gc.conf;
	}
	hcb.exit(gc,env);
	synchronized(CriticalMutex) {
	    Hot_GroupContext.release(gc);
	}
    }

    /** Mainloop of the process
    */
    public void run() {
	trace("Hot_Ensemble::run() started");
		
	int[] cs = new int[1];
	int[] groupID = new int[1];
	int[] cbType = new int[1];
	while(running) {
	    try {
		cs[0] = 0;
		trace("Hot_Ensemble::run() before first read...");
		hioc.read_groupID(cs,groupID);
		trace("CALLBACK: group ID: " + groupID[0]);
		
		hioc.read_cbType(cs,cbType);
		trace("CALLBACK: cb type: " + cbType[0]);
		
		if (cbType[0] > 5 || cbType[0] < 0) {
		    panic("HOT: bad callback type: " + cbType[0]);
		}
		switch (cbType[0]) {
		case 0:    // cb_View
		    cb_View(groupID[0],cs);
		    break;
		case 1:    // cb_Cast
		    cb_Cast(groupID[0],cs);
		    break;
		case 2:    // cb_Send
		    cb_Send(groupID[0],cs);
		    break;
		case 3:    // cb_Heartbeat
		    cb_Heartbeat(groupID[0],cs);
		    break;
		case 4:    // cb_Block
		    cb_Block(groupID[0],cs);
		    break;
		case 5:    // cb_Exit
		    cb_Exit(groupID[0],cs);
		    break;
		default:
		    panic("HOT: really shouldn't be here...");
		}
	    }
	    catch(Exception ex) {
		System.err.println(ex);
	    }
	}
    }
 

    /**
    Halts the application with the error specified by the String
    */
    public static void panic(String s) {
	System.out.println("HOT Panic!: " + s);
	System.exit(45);
    }

	
    /**
    Initializes the critical section mutex
    */
    private void initCritical() {
	CriticalMutex = new Hot_Mutex();
    }
	
	
    /**
    Initializes the write mutex
    */
    private void initWriteCritical() {
	WriteMutex = new Hot_Mutex();
    }


    /**
    Prints (or does not print) the specified string to standard error 
    based upon the debug flag
    */
    public static void trace(String s) {
	if (debug) {
	    System.err.println(s);
	}
    }

// End class Hot_Ensemble
}
