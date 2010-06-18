// $Id: BSH.java,v 1.26 2010/06/18 12:01:54 belaban Exp $

package org.jgroups.protocols;


import bsh.EvalError;
import bsh.Interpreter;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.jgroups.Global;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


/**
 * Beanshell (<a href=http://www.beanshell.org>www.beanshell.org</a>) interpreter class.
 * The <tt>eval()</tt> method receives Java code, executes it and returns the
 * result of the evaluation (or an exception).<p/>
 * This protocol is experimental
 * User: Bela
 * Date: Mar 8, 2003
 * Time: 1:57:07 PM
 * @author Bela Ban
 */
@Experimental @Unsupported
public class BSH extends Protocol implements Runnable {
    protected Interpreter interpreter=null;
    protected ServerSocket srv_sock;
    protected Thread acceptor;
    protected final List<Socket> sockets=new ArrayList<Socket>();


    @Property(description="Port on which the interpreter should listen for requests. 0 is an ephemeral port")
    int bind_port=0;


    public BSH() {
    }

    public void start() throws Exception {
        srv_sock=Util.createServerSocket(getSocketFactory(), Global.BSH_SRV_SOCK, bind_port);
        log.info("Server socket listening at " + srv_sock.getLocalSocketAddress());
        acceptor=new Thread(this);
        acceptor.start();
    }

    public void stop() {
        Util.close(srv_sock);

        if(acceptor != null && acceptor.isAlive())
            acceptor.interrupt();

        Util.sleep(500);
        if(!sockets.isEmpty()) {
            for(Socket sock: sockets)
                Util.close(sock);
        }
    }


    public void run() {
        while(srv_sock != null && !srv_sock.isClosed()) {
            try {
                final Socket sock=srv_sock.accept();
                sockets.add(sock);

                createInterpreter();

                new Thread() {
                    public void run() {
                        try {
                            InputStream input=sock.getInputStream();
                            OutputStream out=sock.getOutputStream();
                            BufferedReader reader=new BufferedReader(new InputStreamReader(input));

                            while(!sock.isClosed()) {
                                String line=reader.readLine();
                                if(line == null || line.length() == 0)
                                    continue;
                                try {
                                    Object retval=interpreter.eval(line);
                                    if(retval != null) {
                                        String rsp=retval.toString();
                                        byte[] buf=rsp.getBytes();
                                        out.write(buf, 0, buf.length);
                                        out.flush();
                                    }
                                    if(log.isTraceEnabled()) {
                                        log.trace(line);
                                        if(retval != null)
                                            log.trace(retval);
                                    }
                                }
                                catch(EvalError evalError) {
                                    evalError.printStackTrace();
                                }
                            }
                        }
                        catch(IOException e) {
                            e.printStackTrace();
                        }
                        finally {
                            Util.close(sock);
                            sockets.remove(sock);
                        }
                    }
                }.start();
            }
            catch(IOException e) {
            }
        }
    }


    synchronized void createInterpreter() {
        // create interpreter just-in-time
        if(interpreter == null) {
            interpreter=new Interpreter();
            try {
                interpreter.set("bsh_prot", this);
            }
            catch(EvalError evalError) {
            }
        }
    }



}
