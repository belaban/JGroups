// $Id: Transport.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups;

public interface Transport {    
    void     send(Message msg) throws Exception;
    Object   receive(long timeout) throws Exception;
}
