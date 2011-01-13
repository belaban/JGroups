package org.jgroups.demos;

import org.jgroups.Address;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.AbstractLockService;
import org.jgroups.blocks.locking.LockNotification;
import org.jgroups.blocks.locking.PeerLockService;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.locks.Lock;

/**
 * Demos the LockService
 */
public class LockServiceDemo implements LockNotification {
    protected String props;
    protected JChannel ch;
    protected AbstractLockService lock_service;
    protected String name;

    public LockServiceDemo(String props, String name) {
        this.props=props;
        this.name=name;
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
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
        List<String> lock_names;
        while(ch.isConnected()) {
            String line=Util.readStringFromStdin(": ");
            if(line.startsWith("quit") || line.startsWith("exit"))
                break;

            if(line.startsWith("lock")) {
                lock_names=parseLockNames(line.substring("lock".length()).trim());
                for(String lock_name: lock_names) {
                    Lock lock=lock_service.getLock(lock_name);
                    lock.lock();
                }
            }
            else if(line.startsWith("unlock")) {
                lock_names=parseLockNames(line.substring("unlock".length()).trim());
                for(String lock_name: lock_names) {
                    if(lock_name.equalsIgnoreCase("all")) {
                        lock_service.unlockAll();
                        break;
                    }
                    else {
                        Lock lock=lock_service.getLock(lock_name);
                        lock.unlock();
                    }
                }
            }
            printLocks();
        }
    }

    protected static List<String> parseLockNames(String line) {
        List<String> lock_names=new ArrayList<String>();
        if(line == null || line.length() == 0)
            return lock_names;
        StringTokenizer tokenizer=new StringTokenizer(line);
        while(tokenizer.hasMoreTokens())
            lock_names.add(tokenizer.nextToken());
        return lock_names;
    }

    protected void printLocks() {
        System.out.println("\n" + lock_service);
    }

    public static void main(String[] args) throws ChannelException {
        String props=null;
        String name=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }

        LockServiceDemo demo=new LockServiceDemo(props, name);
        demo.start();
    }

    protected static void help() {
        System.out.println("LockServiceDemo [-props properties] [-name name]\n" +
                             "Valid commands:\n" +
                             "lock (<lock name>)+" +
                             "unlock (<lock name> | \"ALL\")+");
        System.out.println("\nExample:\nlock lock lock2 lock3\nunlock all");
    }


}
