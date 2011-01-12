package org.jgroups.demos;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.blocks.locking.PeerLockService;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

/**
 * Demos the LockService
 */
public class LockServiceDemo {
    protected String props;
    protected JChannel ch;
    protected LockService lock_service;

    public LockServiceDemo(String props) {
        this.props=props;
    }

    public void start() throws ChannelException {
        ch=new JChannel(props);
        lock_service=new PeerLockService(ch);
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

    protected void loop() throws Exception {
        while(ch.isConnected()) {
            String line=Util.readStringFromStdin(": ");
            if(line.startsWith("quit"))
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
            else {
                System.err.println("Command " + line + " not recognized");
                System.out.println("lock <lock-name>\nunlock <lock-name>\nquit");
            }
        }
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
