package org.jgroups.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Util1_4
 * 
 * @author abollu
 */
public class Util1_4 extends Util
{

	/* double writes are not required.*/
	public static void doubleWriteBuffer(
		ByteBuffer buf,
		WritableByteChannel out)
		throws Exception
	{
		if (buf.limit() > 1)
		{
			int actualLimit = buf.limit();
			buf.limit(1);
			writeFully(buf,out);
			buf.limit(actualLimit);
			writeFully(buf,out);
		}
		else
		{
			buf.limit(0);
			writeFully(buf,out);
			buf.limit(1);
			writeFully(buf,out);
		}
	}

	/*
	 * if we were to register for OP_WRITE and send the remaining data on
	 * readyOps for this channel we have to either block the caller thread or
	 * queue the message buffers that may arrive while waiting for OP_WRITE.
	 * Instead of the above approach this method will continuously write to the
	 * channel until the buffer sent fully.
	 */
	public static void writeFully(ByteBuffer buf, WritableByteChannel out)
		throws IOException
	{
		int written = 0;
		int toWrite = buf.limit();
		while (written < toWrite)
		{
			written += out.write(buf);
		}
	}
}
