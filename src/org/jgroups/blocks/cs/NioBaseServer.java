package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;

import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Bela Ban
 * @since  3.6.5
 */
public abstract class NioBaseServer extends BaseServer {
    protected Selector          selector; // get notified about accepts, data ready to read/write etc
    protected Thread            acceptor; // the thread which calls select() in a loop
    protected final Lock        reg_lock=new ReentrantLock(); // for OP_CONNECT registrations
    protected volatile boolean  registration; // set to true after a registration; the acceptor sets it back to false

    @ManagedAttribute(description="Max number of send buffers. Changing this value affects new buffers only",writable=true)
    protected int               max_send_buffers=5; // size of WriteBuffers send buffer array

    @ManagedAttribute(description="Number of times select() was called")
    protected int               num_selects;

    protected boolean           copy_on_partial_write=true;

    protected long              reader_idle_time=20000;



    protected NioBaseServer(ThreadFactory f, SocketFactory sf, int recv_buf_size) {
        super(f, sf, recv_buf_size);
    }



    public int            maxSendBuffers()              {return max_send_buffers;}
    public NioBaseServer  maxSendBuffers(int num)       {this.max_send_buffers=num; return this;}
    public boolean        selectorOpen()                {return selector != null && selector.isOpen();}
    public boolean        acceptorRunning()             {return acceptor != null && acceptor.isAlive();}
    public int            numSelects()                  {return num_selects;}
    public boolean        copyOnPartialWrite()          {return copy_on_partial_write;}
    public long           readerIdleTime()              {return reader_idle_time;}
    public NioBaseServer  readerIdleTime(long t)        {reader_idle_time=t; return this;}

    public NioBaseServer  copyOnPartialWrite(boolean b) {
        this.copy_on_partial_write=b;
        synchronized(this) {
            for(Connection c: conns.values()) {
                NioConnection conn=(NioConnection)c;
                conn.copyOnPartialWrite(b);
            }
        }
        return this;
    }

    public synchronized int numPartialWrites() {
        int retval=0;
        for(Connection c: conns.values()) {
            NioConnection conn=(NioConnection)c;
            retval+=conn.numPartialWrites();
        }
        return retval;
    }




    /** Prints send and receive buffers for all connections */
    @ManagedOperation(description="Prints the send and receive buffers")
    public String printBuffers() {
        StringBuilder sb=new StringBuilder("\n");
        synchronized(this) {
            for(Map.Entry<Address,Connection> entry: conns.entrySet()) {
                NioConnection val=(NioConnection)entry.getValue();
                sb.append(entry.getKey()).append(":\n  ").append("recv_buf: ").append(val.recv_buf)
                  .append("\n  send_buf: ").append(val.send_buf).append("\n");
            }
        }
        return sb.toString();
    }


    protected SelectionKey register(SelectableChannel ch, int interest_ops, NioConnection conn) throws Exception {
        reg_lock.lock();
        try {
            registration=true;
            selector.wakeup(); // needed because registration will block until selector.select() returns
            return ch.register(selector, interest_ops, conn);
        }
        finally {
            reg_lock.unlock();
        }
    }

    @Override
    protected NioConnection createConnection(Address dest) throws Exception {
        return new NioConnection(dest, this).copyOnPartialWrite(copy_on_partial_write);
    }

    protected void handleAccept(SelectionKey key) throws Exception {
        ;
    }

    protected void acceptorDone() {

    }



    protected class Acceptor implements Runnable {

        public void run() {
            Iterator<SelectionKey> it=null;
            while(running.get() && doSelect()) {
                try {
                    it=selector.selectedKeys().iterator();
                }
                catch(Throwable ex) {
                    continue;
                }

                while(it.hasNext()) {
                    SelectionKey key=it.next();
                    NioConnection conn=(NioConnection)key.attachment();
                    it.remove();
                    try {
                        if(!key.isValid())
                            continue;
                        // a key can be connectable *and* readable (https://issues.redhat.com/browse/JGRP-2531)
                        if(key.isConnectable()) {
                            SocketChannel ch=(SocketChannel)key.channel();
                            if(ch.finishConnect() || ch.isConnected()) {
                                conn.clearSelectionKey(SelectionKey.OP_CONNECT);
                                conn.connected(true);
                            }
                        }
                        else if(key.isAcceptable())
                            handleAccept(key);
                        else {
                            if (key.isReadable())
                                conn.receive();
                            if (key.isWritable())
                                conn.send();
                        }
                    }
                    catch(Throwable ex) {
                        closeConnection(conn);
                    }
                }
            }
            acceptorDone();
        }


        protected boolean doSelect() {
            try {
                int num=selector.select();
                num_selects++;
                checkforPendingRegistrations();
                if(num == 0) return true;
            }
            catch(ClosedSelectorException closed_ex) {
                log.trace("selector was closed; acceptor terminating");
                return false;
            }
            catch(Throwable t) {
                log.warn("acceptor failure", t);
            }
            return true;
        }



        protected void checkforPendingRegistrations() {
            if(registration) {
                reg_lock.lock(); // mostly uncontended -> fast
                try {
                    registration=false;
                }
                finally {
                    reg_lock.unlock();
                }
            }
        }
    }

}
