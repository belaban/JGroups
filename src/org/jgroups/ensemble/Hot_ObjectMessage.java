// $Id: Hot_ObjectMessage.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**

This HOT Message class is a mechanism to transmit Java objects over ensemble.
It makes use of the Java serialization mechanism.  That being said, the usual
rules about an Object being serializable apply (see the Java docs for more 
information here).
	
To send an Object over ensemble, just create a Hot_ObjectMessage with the 
Serializable object, and call the usual Send or Cast.

When your ReceiveCast/Send upcall hands you a Hot_Message reference, you 
create a new Hot_ObjectMessage from that reference, and then do a getObject().

*/

public class Hot_ObjectMessage extends Hot_Message {	
    Object myObj;

    public Hot_ObjectMessage() {
	super();
    }

    public Hot_ObjectMessage(Object o) {
	setObject(o);
    }
	
    /**
    Takes the bytes contained within a Hot_Message object 
    (usually gotten from a standard ReceiveCast/Send upcall)
    and interprets them as a serialized object.
    */
    public Hot_ObjectMessage(Hot_Message msg) {
	this(msg.getBytes());
    }

    /**
    Interprets the bytes as a serialized object
    */
    public Hot_ObjectMessage(byte[] b) {
	setBytes(b);
    }

    /**
    Get the contained Object
    */
    public Object getObject() {
	return myObj;
    }

    /**
    Set the contained Object
    */
    public void setObject(Object o) {
	myObj = o;
    }

    /**
    Serializes the contained object into a byte array
    */
    public byte[] getBytes() {
	try {
   	    ByteArrayOutputStream ostream = new ByteArrayOutputStream(256);
            ObjectOutputStream p = new ObjectOutputStream(ostream);
            p.writeObject(myObj);
	    p.flush();
            byte[] rtnVal = ostream.toByteArray();
            ostream.close();		
	    return rtnVal;
	} catch (Exception e) {
	    e.printStackTrace();
	    Hot_Ensemble.panic("Error in getBytes:Hot_ObjectMessage");
	    return null;
	}
    }
	
    /**
    Interprets the bytes as a serialized object and sets the contained reference
    to the unserialized version of the serialized object
    */
    public void setBytes(byte[] b) {
	try {
	    ByteArrayInputStream is = new ByteArrayInputStream(b);
	    ObjectInputStream p = new ObjectInputStream(is);
	    myObj = p.readObject();
	    is.close();
	} catch (Exception e) {
	    e.printStackTrace();
	    Hot_Ensemble.panic("Error in setBytes:Hot_ObjectMessage");
	}
    }

// End class Hot_ObjectMessage
}
