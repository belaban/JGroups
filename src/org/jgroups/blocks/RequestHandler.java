
package org.jgroups.blocks;


import org.jgroups.Message;


public interface RequestHandler {
    Object handle(Message msg) throws Exception;
}
