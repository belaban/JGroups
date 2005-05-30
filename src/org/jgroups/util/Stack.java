// $Id: Stack.java,v 1.2 2005/05/30 14:31:29 belaban Exp $

package org.jgroups.util;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;



/**
 * LIFO stack, with better performance than java.util.Stack (based on List).
 */
public class Stack extends List {

    
    public Stack() {}


    public void push(Object obj) {
	addAtHead(obj);
    }

    public Object pop() {
	return removeFromHead();
    }

    public Object peek() {
	return peekAtHead();
    }

    
    public Object bottom() {
	return super.peek();
    }


    public boolean empty() {
	return size <= 0;
    }


    public List copy() {
	Stack retval=new Stack();

	synchronized(mutex) {
	    for(Element el=head; el != null; el=el.next)
		retval.add(el.obj);
	}
	return retval;
    }



    public void writeExternal(ObjectOutput out) throws IOException {
	super.writeExternal(out);
    }



    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	super.readExternal(in);
    }





//      public static void main(String[] args) {
//  	Stack   st1=new Stack(), st2;
//  	byte[]  buf;


//  	try {
//  	    for(int i=0; i < 5; i++)
//  		st1.push(new Integer(i));

	    
//  	    buf=Util.objectToByteBuffer(st1);
//  	    st1.pop();
//  	    System.out.println(st1.dump());

//  	    st2=(Stack)Util.objectFromByteBuffer(buf);
//  	    System.out.println(st2.dump());


//  	    st1=(Stack)st2.copy();
//  	    System.out.println(st1.dump());
//  	    System.out.println(st2.dump());
//  	}
//  	catch(Exception e) {
//  	    log.error(e);
//  	}

	

//      }

}
