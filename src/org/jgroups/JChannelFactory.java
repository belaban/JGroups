// $Id: JChannelFactory.java,v 1.1 2003/09/09 01:24:07 belaban Exp $

package org.jgroups;

public class JChannelFactory implements ChannelFactory {
    public Channel createChannel(Object properties) throws ChannelException {
	return new JChannel(properties);
    }
}
