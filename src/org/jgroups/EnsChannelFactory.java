// $Id: EnsChannelFactory.java,v 1.2 2004/07/31 22:14:48 jiwils Exp $

package org.jgroups;

public class EnsChannelFactory implements ChannelFactory {
    String  transport_props=null;
    int     outboard_port=0;


    public EnsChannelFactory() {}


    public EnsChannelFactory(String transport_properties, int outboard_port) {
	setTransport(transport_properties);
	setOutboardPort(outboard_port);
    }


    public void setTransport(String props) {
	transport_props=props;
    }

    public void setOutboardPort(int port) {
	outboard_port=port;
    }



    public Channel createChannel(Object properties) throws ChannelException {
	return new EnsChannel(properties, transport_props, outboard_port);
    }

    /**
     * No-Op implementation of the createChannel() method specified by the
     * <code>Channel</code> interface.
     *
     * @return this implementation always returns <code>null</code>.
     */
    public Channel createChannel() throws ChannelException {
        return null;
    }
}
