// $Id: NetworkTracer.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


import java.io.IOException;
import java.net.Socket;

/**
 * Provides output to a socket. All methods defined here have permissions
 * that are either protected or package.
 * When close is called on this Tracer the socket used by it will also
 * be closed.
 */
public class NetworkTracer extends Tracer
{

    protected Socket out;

    /**
     * Creates a NetworkTracer for a particular module, using the provided socket
     * The constructor is package specific, ie, it can not be used outside this package
     * The constructor expects an already open socket that this tracer will write its output to
     * @param module - the name of the module that uses this NetworkTracer
     * @param level  - the log level as defined in Tracer.java
     * @param socket - an open network socket
     * @see org.jgroups.log.Tracer
     */
    NetworkTracer(String module,
                  int level,
                  Socket socket)
    {
        super(module, level);
        out = socket;
    }

    /**
     * Sends the already-formatted <code>message</code> to the output file,
     * stream, writer, or socket associated with this tracer.
     * In case of an IO exception an error message is printed to the System.err stream
     * @param message a formatted string
     * 
     */
    protected void doPrint(String message)
    {
        try {
            out.getOutputStream().write(message.getBytes());
        }
        catch (IOException e)
        {
            System.err.println("NetworkTracer.doPrint: " + e);
        }
    }

    /**
     * Flushes any pending output. Called from <code>flush</code>, but only
     * if not alread closed.
     * This call flushes the output stream of the socket
     * ie. socket.getOutputStream().flush()
     * In case of an IO error, an error message is sent to System.err
     */
    protected void doFlush()
    {
        try
        {
            out.getOutputStream().flush();
        }
        catch (IOException e)
        {
            System.err.println("NetworkTracer.doFlush: " + e);
        }
    }


    /**
     * Flushes any pending output (by calling <code>flush</code>) and closes
     * the output file, stream, writer, or socket associated with this tracer.
     * Called from <code>close</code>, but only if not alread closed.
     * <p>
     * After closed has been called, all calls to <code>print</code>,
     * <code>flush</code>, and <code>close</code> are ignored.
     * In case of an IO error, an error message is sent to System.err
     */
    protected void doClose()
    {
        try
        {
            out.close();
        }
        catch (IOException e)
        {
            System.err.println("NetworkTracer.doClose: " + e);
        }
    }

}
