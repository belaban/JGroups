// $Id: Hot_IO_Controller.java,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
Defines how Hot_Ensemble talks to the outboard process
Is used by Hot_Ensemble for actual IO
*/

class Hot_IO_Controller {
    // grab the meaningless padding bytes from here...
    // private static byte[] padBytes = new byte[4]; 
    private BufferedInputStream is;
    private OutputStream os;
    private boolean usingChecksum;


    private boolean print=false;


    public Hot_IO_Controller(BufferedInputStream is, OutputStream os) {
	HOT_MAGIC *= 10; // see the definition of the variable below...
	HOT_MAGIC += 9;
	this.is = is;
	this.os = os;
	w_error = 0;
	r_error = 0;
	usingChecksum = false;
    }


    public final int DN_JOIN = 0;
    public final int DN_CAST = 1;
    public final int DN_SEND = 2;
    public final int DN_SUSPECT = 3;
    public final int DN_PROTOCOL = 4;
    public final int DN_PROPERTIES = 5;
    public final int DN_LEAVE = 6;
    public final int DN_PROMPT = 7;
    public final int DN_BLOCK_ON = 8;

    public final int CB_VIEW = 0;
    public final int CB_CAST = 1;
    public final int CB_SEND = 2;
    public final int CB_HEARTBEAT = 3;
    public final int CB_BLOCK = 4;
    public final int CB_EXIT = 5;

    private int HOT_MAGIC = 365718590/*9*/;  // must be same as in ML
    // In Java the primitive int is 32 bits.
    public final int INT_SIZE = 4;	     // a 32 bit integer

    int w_error, r_error;		     // package-scope read/write errors

    public Hot_Error check_write_errors() {
	if (w_error!= 0) {
	    w_error = 0;
	    return new Hot_Error(0,"HOT: write failed");
	}
	return null;
    }

    public Hot_Error check_read_errors() { 
	if (r_error != 0) {
	    r_error = 0;
	    return new Hot_Error(0,"HOT: read failed");
	}
	return null; 
    }

    public void write_checksum(int checksum) { 
	if (usingChecksum) {
	    int u = checksum ^ HOT_MAGIC;
            if (w_error != 0) return;
	    byte[] pBytes = new byte[4];
	    try {
		for (int i = 3; i >= 0; i--) {
		    pBytes[i] = (byte)u;
		    u = (u >> 8);
		}
		os.write(pBytes);
		os.flush();
	    } catch (Exception e) {
	        w_error = 1;
                e.printStackTrace();
	    }
	}
	return; 
    }

    public void write_uint(int[] checksum, int u) {
	int holder = u;
	if (w_error != 0) return;
	byte[] pBytes = new byte[4];
	try {
	    for (int i = 3; i >= 0; i--) {
		pBytes[i] = (byte)u;
		u = (u >> 8);
	    }
	    os.write(pBytes);
	    os.flush();
            Hot_Ensemble.trace("write_uint: wrote " + holder);
	    try {
		checksum[0]+= INT_SIZE;
	    } catch (Exception e) {
		System.err.println("Hot_IO_Controller::write_uint: no checksum passed in");
	    }
	} catch (Exception e) {
	    w_error = 1;
	    e.printStackTrace();
	}
	return; 
    }

    public void write_bool(int[] checksum, boolean b) { 
	if (w_error != 0) return;
	write_uint(checksum,b?1:0);
	return; 
    }

    public void write_buffer(int[] checksum, Hot_Buffer b) { 
	byte[] pBytes = b.getBytes();
	write_actual_buffer(checksum,pBytes);
    }
	
    public void write_actual_buffer(int[] checksum, byte[] pBytes) {
	byte[] padBytes=new byte[INT_SIZE];
       
	if (w_error != 0) return;
	// writing:
	// number of bytes in the buffer (4 byte int)
	// the buffer
	// padding (buffer_size % INT_SIZE bytes)
	int l = pBytes.length;

	int padSize = INT_SIZE - (l % INT_SIZE);

	if(padSize == INT_SIZE)
	    padSize=0;

	// System.out.println("write_actual_buffer: len=" + l + ", padSize=" + padSize);

	write_uint(checksum, l);
	if (w_error != 0) return;

	try {
	    Hot_Ensemble.trace("Buffer: length = " + l + " pad = " + padSize);
	    Hot_Ensemble.trace("Buffer: sent: " + new String(pBytes));

	    os.write(pBytes);
	    // System.out.println("Wrote " + pBytes.length + " bytes");
	    os.flush();

	    // writeToStream(os, pBytes);


	    if ((padSize > 0) && (padSize < INT_SIZE)) {
		os.write(padBytes,0,padSize);
		// System.out.println("Wrote " + padSize + " pad bytes");
		os.flush();
	    }
	    try {
		checksum[0]+= l + padSize;
	    } catch (Exception e) {
		System.err.println("Hot_IO_Controller::write_buffer: no checksum passed in");
	    }
	} catch (Exception e) {
	    w_error = 1;
	    e.printStackTrace();
	}
	return; 
    }



    public void write_string(int[] checksum, String s) { 
	byte[] pBytes;
	pBytes = s.getBytes();	// lossy!!! Ensemble wants ASCII, we have UNICODE
	write_actual_buffer(checksum,pBytes);
    }

    public void write_endpID(int[] checksum, Hot_Endpoint epid) { 
	write_string(checksum, epid.name);
	return; 
    }

    public void write_groupID(int[] checksum, int gid) { 
	write_uint(checksum, gid);
	return; 
    }




    public void write_msg(int[] checksum, Hot_Message msg) { 
	if (w_error != 0) return;
	int[] pInt = new int[1];
	// we are sending:
	// 4 bytes representing the total number of bytes (minus this 4) being 
        // sent some padding (x bytes where x = msg.length() % INT_SIZE), first
	// byte of padding is a number telling how many bytes of padding there 
	// are. (I think this might be a bug...)
	// (i.e. what if the message being sent doesn't need any padding, and 
	// the first byte of the message is 0x01 (or 0x02, 0x03 for that matter)?)

	byte[] pBytes = msg.getBytes();
	int l = pBytes.length;
	
	byte nPadding = (byte)(INT_SIZE - (l % INT_SIZE));
	byte padBytes[]=new byte[INT_SIZE];

	write_uint(checksum, l + nPadding);
	if (w_error != 0) Hot_Ensemble.panic("write_msg: error writing msg length");
	try {
   	    Hot_Ensemble.trace("Msg: sending: " + new String(pBytes));
	    Hot_Ensemble.trace("Msg: length = " + l + " pad = " + nPadding);
	
  	    if (nPadding != (byte)0) {
		padBytes[0] = nPadding;
		os.write(padBytes,0,nPadding);
		os.flush();
	    }

	    os.write(pBytes);
	    os.flush();

	    // writeToStream(os, pBytes);


	    try {
		checksum[0]+= INT_SIZE;
	    } catch (Exception e) {
		System.err.println("Hot_IO_Controller::write_msg: no checksum passed in");
	    }
	} catch (Exception e) {
	    w_error = 1;
	    e.printStackTrace();
	}
	return;
    }









    public void write_endpList(int[] checksum, Hot_Endpoint[] epids) { 
	if (w_error != 0) return;
	int l = epids.length;
	write_uint(checksum,l);
	for(int i = 0;i<l;i++)
	    write_endpID(checksum, epids[i]);
	return; 
    }

    public void write_dnType(int[] checksum, int type) { 
	if (w_error != 0) return;
	write_uint(checksum,type);
	return; 
    }


    /* Write in small packets */
    private void writeToStream(OutputStream out, byte[] buf) {
	int MAX_SIZE=128;
	int len=buf.length;
	int written=0;
	int bytes_to_write=0;
	try {
	    if(len < MAX_SIZE) {
		out.write(buf);
		return;
	    }

	    while(written < len) {

		if((len - written) > MAX_SIZE)
		    bytes_to_write=MAX_SIZE;
		else
		    bytes_to_write=len - written;

		// System.out.println("WriteToStream: writing " + bytes_to_write + " bytes");
		out.write(buf, written, bytes_to_write);
		written+=bytes_to_write;
	    }
	    out.flush();

	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }


    private int readIntoBuffer(InputStream is, byte[] buf) {
	int actual_length=buf.length;
	int tmp=0;
	int numbytes=0;

	while(true) {
	    try {
		tmp=is.read(buf, numbytes, actual_length-numbytes);
		if(tmp == -1)
		    break;
		// System.out.println("ReadIntoBuffer: read " + tmp + " bytes");
		numbytes+=tmp;
		
		if(numbytes < actual_length)
		    continue;
		else
		    break;		    
	    }
	    catch(Exception e) {
		System.err.println(e);
		break;
	    }
	}
	return numbytes;
    }



    public void read_checksum(int checksum) { 
	if (usingChecksum) {
	    if (r_error != 0) return;
	    try {
		byte[] pBytes = new byte[4];
		is.read(pBytes);
		int tmpInt = pBytes[0];
		for(int i=1;i<4;i++) {
		    tmpInt = (tmpInt << 8);
		    tmpInt |= pBytes[i];
		}
		if ((tmpInt ^ HOT_MAGIC) != checksum) {
		    Hot_Ensemble.panic("read_checksum: " + (tmpInt^HOT_MAGIC) + 
					" (expected " + checksum + ")");
		}
	    } catch (Exception e) {
		r_error = 1;
		e.printStackTrace();
	    }
	}
	return; 
    }

    public void read_uint(int[] checksum, int[] u) { 
	if (r_error != 0) {
	    System.err.println("read_uint: there was a read eror !");
	    return;
	}
	try {
	    byte[] pBytes = new byte[4];

	    for(int i=0; i < pBytes.length; i++)
		pBytes[i]=0;

	    is.read(pBytes);

	    if(print) {
		System.out.print("\nread_uint: bytes read are ");
		for(int i=0; i < pBytes.length; i++) {
		    System.out.print(pBytes[i] + " ");
		}
		System.out.println("");
	    }

            Hot_Ensemble.trace("Bytes::" + (int)pBytes[0] + " " + 
		(int)pBytes[1] + " " + (int)pBytes[2] + " " + (int)pBytes[3]);


// 	    int tmpInt = (int)pBytes[0];
// 	    for(int i=1;i<4;i++) {
// 		tmpInt = (tmpInt << 8);
// 		tmpInt |= pBytes[i];
// 	    }



	    int tmpInt=0;
	    int b1=pBytes[0], b2=pBytes[1], b3=pBytes[2], b4=pBytes[3];
	    
	    
	    if(b1 < 0) b1=256+b1;
	    if(b2 < 0) b2=256+b2;
	    if(b3 < 0) b3=256+b3;
	    if(b4 < 0) b4=256+b4;       	    
	    tmpInt=(b1 << 24) + (b2 << 16) + (b3 << 8) + b4;




	    u[0] = tmpInt;

	    if(print)
		System.out.println("tmpInt is " + u[0]);

	    try {
		checksum[0]+= INT_SIZE;
	    } catch (Exception e) {
		System.err.println("Hot_IO_Controller::read_uint: no checksum passed in");
	    }
	    Hot_Ensemble.trace("read_uint: " + u[0]);
	} catch (Exception e) {
		r_error = 1;
		e.printStackTrace();
	}
	return; 
    }



    public void read_bool(int[] checksum, boolean[] b) {
	if (r_error != 0) return;
	int[] anInt = new int[1];
	read_uint(checksum, anInt);
	b[0] = (anInt[0] == 1) ? true : false;
	return; 
    }

    public void read_buffer(int[] checksum, Hot_Buffer b) { 
	if (r_error != 0) return;
	int[] bufSize = new int[1];
	read_uint(checksum, bufSize);
	if (r_error != 0) return;

	byte[] pBytes = new byte[bufSize[0]];
	byte[] padBytes=new byte[INT_SIZE];

	try {

	    // is.read(pBytes);
	    
	    readIntoBuffer(is, pBytes);


	    Hot_Ensemble.trace("read_buffer: read in bytes: " + pBytes.length);
	    b.setBytes(pBytes);


	    int padSize = INT_SIZE - (bufSize[0] % INT_SIZE);

	    if(padSize == INT_SIZE)
		padSize=0;

	    // int padSize = bufSize[0] % INT_SIZE;


	    // System.out.println("read_buffer: padSize=" + padSize);


	    if ((padSize > 0) && (padSize < INT_SIZE)) {
		is.read(padBytes,0, padSize);
		Hot_Ensemble.trace("read_buffer: read in buffer bytes: " + padSize);
	    }
	    try {
		// INT_SIZE addition is taken care of in read_uint
		checksum[0]+= bufSize[0] + (bufSize[0] % INT_SIZE);  
		// checksum[0]+= INT_SIZE + bufSize[0] + padSize;  
	    } catch (Exception e) {
		System.err.println("Hot_IO_Controller::read_buffer: no checksum passed in");
	    }
	} catch (Exception e) {
	    r_error = 1;
	    System.err.println("Hot_IO_Controller::read_buffer: reading: " + e.toString());
	    e.printStackTrace();
	}
	return; 
    }



    public void read_string(int[] checksum, String[] sb) { 
	if (r_error != 0) return;
	Hot_Buffer hb = new Hot_Buffer();
	read_buffer(checksum,hb);
	hb.toAsciiString(sb);
	return; 
    }

    public void read_endpID(int[] checksum, Hot_Endpoint epid) { 
	if (r_error != 0) return;
	String[] pString = new String[1];
	read_string(checksum, pString);
	if (r_error != 0) return;
	epid.name = pString[0];
	return; 
    }

    public void read_groupID(int[] checksum, int[] gid) { 
	if (r_error != 0) return;
	read_uint(checksum,gid);
	return; 
    }





    public void read_msg(int[] checksum, Hot_Message msg) { 
	if (r_error != 0) return;
	int[] msgSize = new int[1];
	int   numbytes=0;

	print=true;
	read_uint(checksum, msgSize);
	print=false;


	if (r_error != 0) {
	    System.err.println("read_msg: read error !");
	    return;
	}

	try {
	    byte[] pad = new byte[INT_SIZE];
	    is.read(pad,0,1);
	    int npad = pad[0];
	    if ((npad <= 0) || (npad >= 5)) {
		Hot_Ensemble.panic("error in padding of read_msg");
	    }
	    // read rest of padding
	    if (npad > 1) {
		is.read(pad,0,npad-1);
	    }

	    int len=msgSize[0] - npad;
	    byte[] pBytes = new byte[msgSize[0] - npad];

	    // System.out.println("read_msg(): msg size=" + len + ", pad size=" + npad);
	    
	    int avail=is.available();
	    // System.out.println("Available bytes: " + avail);

	    numbytes=readIntoBuffer(is, pBytes);

	    // System.out.println(numbytes + " were read");
	    if(numbytes == -1) {
		System.err.println("read_msg received -1 bytes !");
		System.exit(-1);
	    }

	    if(numbytes != len) {
		System.err.println("Wanted to read " + len + " bytes, but read " +
				   numbytes + " bytes");
		System.exit(-1);
	    }
		

	    msg.setBytes(pBytes);
	    try {
		checksum[0]+= msgSize[0] + npad;
	    } catch (Exception e) {
		System.err.println("Hot_IO_Controller::read_msg: no checksum passed in");
            }
	} catch (Exception e) {
	    r_error = 1;
            System.err.println("Hot_IO_Controller::read_msg: reading: " + e.toString());
	    e.printStackTrace();
	}
	return; 
    }





    public Hot_Endpoint[] read_endpList(int[] checksum, int[] size) { 
	Hot_Endpoint[] rtnVal = null;
	if (r_error != 0) return rtnVal;
	read_uint(checksum,size);
	if (r_error != 0) return rtnVal;
	rtnVal = new Hot_Endpoint[size[0]];
	for (int i = 0; i < size[0]; i++) {
	    rtnVal[i] = new Hot_Endpoint();
	    read_endpID(checksum,rtnVal[i]);
	    if (r_error != 0) {
		Hot_Ensemble.trace("Hot_IO_Controller::read_endpList: error in read_endpID");
		return null;
	    }
	}
	return rtnVal; 
    }

    public boolean[] read_boolList(int[] checksum) {
	if (r_error != 0) return null;
	int[] numBools = new int[1];
	read_uint(checksum,numBools);
	boolean[] rtnVal = new boolean[numBools[0]];
	if (r_error != 0) return null;
	boolean[] b = new boolean[1];
	for (int i = 0;i < numBools[0];i++) {
	    read_bool(checksum,b);
	    if (r_error != 0) {
		Hot_Ensemble.trace("Hot_IO_Controller::read_boolList: error in read_bool");
		return null;
	    }
	    rtnVal[i] = b[0];
	}
	return rtnVal;
    }

    public void read_cbType(int[] checksum, int[] type) {
	type[0]=0;

	if (r_error != 0) {
	    System.out.println("read_cbType: there was a read error !");
	    return;
	}

	// print=true;
	read_uint(checksum,type);
	// print=false;

	// System.out.println("cbType --> " + type[0]);

	return; 
    }

// End class Hot_IO_Controller 
}
