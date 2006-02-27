package org.jgroups.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.ChannelException;
import org.jgroups.conf.ClassConfigurator;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

/**
 * Uses magic numbers for class descriptors
 * @author Bela Ban
 * @version $Id: MagicObjectOutputStream.java,v 1.5 2006/02/27 14:09:57 belaban Exp $
 */
public class MagicObjectOutputStream extends ObjectOutputStream {
    static volatile ClassConfigurator conf=null;
    static final Log log=LogFactory.getLog(MagicObjectOutputStream.class);


    public MagicObjectOutputStream(OutputStream out) throws IOException {
        super(out);
        if(conf == null) {
            try {
                conf=ClassConfigurator.getInstance(false);
            }
            catch(ChannelException e) {
                log.error("ClassConfigurator could not be instantiated", e);
            }
        }
    }

    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
        int magic_num;
        if(conf == null) {
            super.writeInt(-1);
            super.writeClassDescriptor(desc);
            return;
        }
        magic_num=conf.getMagicNumberFromObjectStreamClass(desc);
        super.writeInt(magic_num);
        if(magic_num == -1) {
            if(log.isTraceEnabled()) // todo: remove
                log.trace("could not find magic number for '" + desc.getName() + "': writing full class descriptor");
            super.writeClassDescriptor(desc);
        }
        else {
            //if(log.isTraceEnabled())
               // log.trace("writing descriptor (num=" + magic_num + "): " + desc.getName());
        }
    }

}

