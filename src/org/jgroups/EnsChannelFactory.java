// $Id: EnsChannelFactory.java,v 1.1 2003/09/09 01:24:07 belaban Exp $

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
}
