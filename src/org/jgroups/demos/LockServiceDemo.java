package org.jgroups.demos;

import org.jgroups.Address;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.AbstractLockService;
import org.jgroups.blocks.locking.LockNotification;
import org.jgroups.blocks.locking.PeerLockService;
import org.jgroups.util.Util;

import java.util.concurrent.locks.Lock;

/**
 * Demos the LockService
 */
public class LockServiceDemo implements LockNotification {
    protected String props;
    protected JChannel ch;
    protected AbstractLockService lock_service;

    public LockServiceDemo(String props) {
        this.props=props;
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        lock_service=new PeerLockService(ch);
        lock_service.addLockListener(this);
        ch.connect("lock-cluster");
        try {
            loop();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            Util.close(ch);
        }
    }

    public void lockCreated(String name) {
        System.out.println("lock \"" + name + "\" was created");
    }

    public void lockDeleted(String name) {
        System.out.println("lock \"" + name + "\" was deleted");
    }

    public void locked(String lock_name, Address owner) {
        System.out.println("\"" + lock_name + "\" locked by " + owner);
    }

    public void unlocked(String lock_name, Address owner) {
        System.out.println("\"" + lock_name + "\" unlocked by " + owner);
    }
    

    protected void loop() throws Exception {
        while(ch.isConnected()) {
            String line=Util.readStringFromStdin(": ");
            if(line.startsWith("quit") || line.startsWith("exit"))
                break;
            if(line.startsWith("lock")) {
                String lock_name=line.substring("lock".length()).trim();
                Lock lock=lock_service.getLock(lock_name);
                lock.lock();
            }
            else if(line.startsWith("unlock")) {
                String lock_name=line.substring("unlock".length()).trim();
                Lock lock=lock_service.getLock(lock_name);
                lock.unlock();
            }

            printLocks();
        }
    }

    protected void printLocks() {
        System.out.println("\n" + lock_service);
    }

    public static void main(String[] args) throws ChannelException {
        String props=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }

        LockServiceDemo demo=new LockServiceDemo(props);
        demo.start();
    }

    protected static void help() {
        System.out.println("LockServiceDemo [-props properties]");
    }


}
