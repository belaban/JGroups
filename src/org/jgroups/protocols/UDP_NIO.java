package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.LogicalAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.*;
import java.util.*;


interface Receiver {

    /** Called when data has been received on a socket. When the callback returns, the buffer will be
     * reused: therefore, if <code>buf</code> must be processed on a separate thread, it needs to be copied.
     * This method might be called concurrently by multiple threads, so it has to be reentrant
     * @param packet
     */
    void receive(DatagramPacket packet);
}
