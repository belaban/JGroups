// $Id: WriterTracer.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.log;


import java.io.IOException;
import java.io.Writer;

/**
 * Provides output to a <code>Writer</code>. All methods defined
 * here have permissions that are either protected or package.
 */
public class WriterTracer extends Tracer 
{

    protected Writer out;

    /**
     * Creates a WriteTracer for a particular module, using the provided writer
     * The constructor is package specific, ie, it can not be used outside this package
     * @param module - the name of the module that uses this WriteTracer
     * @param level  - the log level as defined in Tracer.java
     * @param stream - a print stream that will be printed to
     * @see org.jgroups.log.Tracer
     */
    WriterTracer(String module, int level, Writer writer) 
    {
        super(module, level);
        out = writer;
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
        try 
        {
            out.write(message);
        }
        catch (IOException e) 
        {
            System.err.println("WriterTracer.doPrint: "
                               + "while attempting to write message '" + message
                               + "', saw exception " + e);
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
            out.flush();
        }
        catch (IOException e) 
        {
            // System.err.println("WriterTracer.doFlush: " + e);
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
            System.err.println("WriterTracer.doClose: " + e);
        }
    }

}
