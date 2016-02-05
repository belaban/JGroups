
package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface to provide state (state provider) and set state (state requester)
 * @since 4.0
 * @author Bela Ban
 * @author Vladimir Blagojevic
 */
public interface StateListener {


   /**
    * Allows an application to write a state through a provided OutputStream. After the state has
    * been written the OutputStream doesn't need to be closed as stream closing is automatically
    * done when a calling thread returns from this callback.
    *
    * @param output
    *           the OutputStream
    * @throws Exception
    *            if the streaming fails, any exceptions should be thrown so that the state requester
    *            can re-throw them and let the caller know what happened
    * @see OutputStream#close()
    */
    void getState(OutputStream output) throws Exception;

   /**
    * Allows an application to read a state through a provided InputStream. After the state has been
    * read the InputStream doesn't need to be closed as stream closing is automatically done when a
    * calling thread returns from this callback.
    *
    * @param input
    *           the InputStream
    * @throws Exception
    *            if the streaming fails, any exceptions should be thrown so that the state requester
    *            can catch them and thus know what happened
    * @see InputStream#close()
    */
    void setState(InputStream input) throws Exception;
}
