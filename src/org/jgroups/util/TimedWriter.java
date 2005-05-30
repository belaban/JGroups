// $Id: TimedWriter.java,v 1.5 2005/05/30 16:14:45 belaban Exp $

package org.jgroups.util;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
   Waits until the buffer has been written to the output stream, or until timeout msecs have elapsed,
   whichever comes first. 
   TODO: make it more generic, so all sorts of timed commands should be executable. Including return
   values, exceptions and Timeout exception. Also use ReusableThread instead of creating a new threa 
   each time.

   @author  Bela Ban
*/


public class TimedWriter {
    Thread        thread=null;
    long          timeout=2000;
    boolean       completed=true;
    Exception     write_ex=null;
    Socket        sock=null;
    static Log    log=LogFactory.getLog(TimedWriter.class);


    class Timeout extends Exception {
	public String toString() {
	    return "TimedWriter.Timeout";
	}
    }



    class WriterThread extends Thread {
	DataOutputStream  out=null;
	byte[]            buf=null;
	int               i=0;

	
	public WriterThread(OutputStream out, byte[] buf) {
	    this.out=new DataOutputStream(out);
	    this.buf=buf;
	    setName("TimedWriter.WriterThread");
	}
	
	public WriterThread(OutputStream out, int i) {
	    this.out=new DataOutputStream(out);
	    this.i=i;
	    setName("TimedWriter.WriterThread");
	}

	public void run() {
	    try {
		if(buf != null)
		    out.write(buf);
		else {
		    out.writeInt(i);
		}
		    
	    }
	    catch(IOException e) {
		write_ex=e;
	    }
	    completed=true;
	}
    }


    class SocketCreator extends Thread {
	InetAddress local=null, remote=null;
	int         peer_port=0;

	
	public SocketCreator(InetAddress local, InetAddress remote, int peer_port) {
	    this.local=local;
	    this.remote=remote;
	    this.peer_port=peer_port;
	}


	public void run() {
	    completed=false;
	    sock=null;

	    try {
		sock=new Socket(remote, peer_port, local, 0); // 0 means choose any port
	    }
	    catch(IOException io_ex) {
		write_ex=io_ex;
	    }
	    completed=true;
	}
    }




    void start(InetAddress local, InetAddress remote, int peer_port) {
	stop();
	thread=new SocketCreator(local, remote, peer_port);
	thread.start();
    }


    void start(OutputStream out, byte[] buf) {
	stop();
	thread=new WriterThread(out, buf);
	thread.start();
    }


    void start(OutputStream out, int i) {
	stop();
	thread=new WriterThread(out, i);
	thread.start();
    }



    void stop() {
	if(thread != null && thread.isAlive()) {
	    thread.interrupt();
	    try {thread.join(timeout);}
	    catch(Exception e) {}
	}
    }

    
    /**
       Writes data to an output stream. If the method does not return within timeout milliseconds,
       a Timeout exception will be thrown.
     */
    public synchronized void write(OutputStream out, byte[] buf, long timeout)
	throws Exception, Timeout, InterruptedException {
	if(out == null || buf == null) {
	    log.error("TimedWriter.write(): output stream or buffer is null, ignoring write");
	    return;
	}

	try {
	    this.timeout=timeout;
	    completed=false;
	    start(out, buf);	    
	    if(thread == null) 
		return;
	    
	    thread.join(timeout);

	    if(completed == false) {
		throw new Timeout();
	    }
	    if(write_ex != null) {
		Exception tmp=write_ex;
		write_ex=null;
		throw tmp;
	    }
	}
	finally {   // stop the thread in any case
	    stop();
	}
    }


    public synchronized void write(OutputStream out, int i, long timeout) 
	throws Exception, Timeout, InterruptedException {
	if(out == null) {
	    log.error("TimedWriter.write(): output stream is null, ignoring write");
	    return;
	}

	try {
	    this.timeout=timeout;
	    completed=false;
	    start(out, i);
	    if(thread == null) 
		return;
	    
	    thread.join(timeout);
	    if(completed == false) {
		throw new Timeout();
	    }
	    if(write_ex != null) {
		Exception tmp=write_ex;
		write_ex=null;
		throw tmp;
	    }
	}
	finally {   // stop the thread in any case
	    stop();
	}
    }


    /** Tries to create a socket to remote_peer:remote_port. If not sucessful within timeout
	milliseconds, throws the Timeout exception. Otherwise, returns the socket or throws an
	IOException. */
    public synchronized Socket createSocket(InetAddress local, InetAddress remote, int port, long timeout) 
	throws Exception, Timeout, InterruptedException {

	try {
	    this.timeout=timeout;
	    completed=false;
	    start(local, remote, port);
	    if(thread == null) 
		return null;
	    
	    thread.join(timeout);
	    if(completed == false) {
		throw new Timeout();
	    }
	    if(write_ex != null) {
		Exception tmp=write_ex;
		write_ex=null;
		throw tmp;
	    }
	    return sock;
	}
	finally {   // stop the thread in any case
	    stop();
	}
    }




    public static void main(String[] args) {
        TimedWriter  w=new TimedWriter();
        InetAddress  local=null;
        InetAddress  remote=null;
        int          port=0;
        Socket       sock=null ;

        if(args.length != 3) {
            log.error("TimedWriter <local host> <remote host> <remote port>");
            return;
        }

        try {
            local=InetAddress.getByName(args[0]);
            remote=InetAddress.getByName(args[1]);
            port=Integer.parseInt(args[2]);
        }
        catch(Exception e) {
            log.error("Could find host " + remote);
            return;
        }

        while(true) {

            try {
                sock=w.createSocket(local, remote, port, 3000);
                if(sock != null) {
                    System.out.println("Connection created");
                    return;
                }
            }
            catch(TimedWriter.Timeout t) {
                log.error("Timed out creating socket");
            }
            catch(Exception io_ex) {
                log.error("Connection could not be created, retrying");
                Util.sleep(2000);
            }
        }

    }
}
