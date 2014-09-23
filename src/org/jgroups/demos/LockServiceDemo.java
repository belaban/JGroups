package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockNotification;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Owner;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Demos the LockService
 */
public class LockServiceDemo implements LockNotification {
    protected String props;
    protected JChannel ch;
    protected LockService lock_service;
    protected String name;

    public LockServiceDemo(String props, String name) {
        this.props=props;
        this.name=name;
    }

    public void start() throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        lock_service=new LockService(ch);
        lock_service.addLockListener(this);
        ch.connect("lock-cluster");
        JmxConfigurator.registerChannel(ch, Util.getMBeanServer(), "lock-service", ch.getClusterName(), true);

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
    }

    public void lockDeleted(String name) {
    }

    public void locked(String lock_name, Owner owner) {
        System.out.println("\"" + lock_name + "\" locked by " + owner);
    }

    public void unlocked(String lock_name, Owner owner) {
        System.out.println("\"" + lock_name + "\" unlocked by " + owner);
    }

    public void awaiting(String lock_name, Owner owner) {
        System.out.println("awaiting \"" + lock_name + "\" by " + owner);
    }

    public void awaited(String lock_name, Owner owner) {
        System.out.println("awaited \"" + lock_name + "\" by " + owner);
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
            else  if(line.startsWith("trylock")) {
                lock_names=parseLockNames(line.substring("trylock".length()).trim());

                String tmp=lock_names.get(lock_names.size() -1);
                Long timeout=(long)-1;
                try {
                    timeout=Long.parseLong(tmp);
                    lock_names.remove(lock_names.size() -1);
                }
                catch(NumberFormatException e) {
                }

                for(String lock_name: lock_names) {
                    Lock lock=lock_service.getLock(lock_name);
                    boolean rc;
                    if(timeout < 0)
                        rc=lock.tryLock();
                    else
                        rc=lock.tryLock(timeout, TimeUnit.MILLISECONDS);
                    if(!rc)
                        System.err.println("Failed locking \"" + lock_name + "\"");
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
                        if(lock != null)
                            lock.unlock();
                    }
                }
            }
            else if(line.startsWith("view"))
                System.out.println("View: " + ch.getView());
            else if(line.startsWith("help"))
                help();
            printLocks();
        }
    }

    protected static List<String> parseLockNames(String line) {
        List<String> lock_names=new ArrayList<String>();
        if(line == null || line.isEmpty())
            return lock_names;
        StringTokenizer tokenizer=new StringTokenizer(line);
        while(tokenizer.hasMoreTokens())
            lock_names.add(tokenizer.nextToken());
        return lock_names;
    }

    protected void printLocks() {
        System.out.println("\n" + lock_service.printLocks());
    }



    public static void main(String[] args) throws Exception {
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
        System.out.println("\nLockServiceDemo [-props properties] [-name name]\n" +
                             "Valid commands:\n\n" +
                             "lock (<lock name>)+\n" +
                             "unlock (<lock name> | \"ALL\")+\n" +
                             "trylock (<lock name>)+ [<timeout>]\n");
        System.out.println("Example:\nlock lock lock2 lock3\nunlock all\ntrylock bela michelle 300");
    }

}
