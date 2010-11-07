package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Bela Ban
 * @version $Id$
 */
public class bla3 {
    public static void main(String[] args) throws UnknownHostException {
        InetAddress addr=InetAddress.getByName(args[0]);
        System.out.println("addr = " + addr);
    }

}
