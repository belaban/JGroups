package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * An adapter implementing the Receiver interface with no-op implementations. When implementing a
 * callback, we can simply extend ReceiverAdapter and overwrite receive() in order to not having to
 * implement all callbacks of the interface.
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class ReceiverAdapter implements Receiver {

   /**
    * {@inheritDoc}
    */
   public void receive(Message msg) {
    }

   /**
    * {@inheritDoc}
    */
    public void getState(OutputStream output) throws Exception {
    }

   /**
    * {@inheritDoc}
    */
    public void setState(InputStream input) throws Exception {
    }

   /**
    * {@inheritDoc}
    */
    public void viewAccepted(View view) {
    }

   /**
    * {@inheritDoc}
    */
    public void suspect(Address mbr) {
    }

   /**
    * {@inheritDoc}
    */
    public void block() {
    }

   /**
    * {@inheritDoc}
    */
    public void unblock() {
    }
}
