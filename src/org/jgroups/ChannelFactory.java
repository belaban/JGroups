// $Id: ChannelFactory.java,v 1.1.1.1 2003/09/09 01:24:07 belaban Exp $

package org.jgroups;


/**
   A channel factory takes care of creation of channel implementations. Subclasses will create
   different implementations.
 */
public interface ChannelFactory {

    /**
       Creates an instance implementing the <code>Channel</code> interface.
       @param properties The specification of the protocol stack (underneath the channel).
              A <code>null</code> value means use the default properties.
       @exception ChannelException Thrown when the creation of the channel failed, e.g.
                  the <code>properties</code> specified were incompatible (e.g. a missing
		  UDP layer etc.)
     */
    Channel createChannel(Object properties) throws ChannelException;
}
