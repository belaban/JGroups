// $Id: RWLock.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


/** 
 * Lock allowing multiple reads or a single write. Waiting writes have 
 * priority over new reads. 
 * <p>
 * Code derived from com.sun.jini.thread.ReadersWriter, 
 * Jini 1.1, Sun Microsystems
 */
public class RWLock {
	/** Interrupted exception wrapped in a RuntimeException */
	public static class IntException extends RuntimeException {
		public IntException() { super(); }
		public IntException(String msg) { super(msg); }
	}
	
	/** Exception thrown when a lock request would block the caller */
	public static class BlockException extends Exception {
		public BlockException() { super(); }
		public BlockException(String msg) { super(msg); }
	}
	
	
	/** Number of active read locks */
	private int _reads;
	/** Number of pending write lock requests */
	private int _waitWrites;
	/** Whether the write lock is held */
	private boolean _write;
	
	
	public RWLock() { super(); 
		_reads      = 0; 
		_waitWrites = 0; 
		_write      = false; 
	}
	
	
	/** 
	 * Obtain a read lock 
	 * 
	 * @throws IntException if interrupted while waiting on the lock
	 */
	public synchronized void readLock() { 
		while(_write || _waitWrites != 0) {
			try { wait(); 
			} catch(InterruptedException ex) {
			throw new IntException(); 
			}
		}
		++_reads; 
	}
	
	/** 
	 * Revoke the read lock 
	 */
	public synchronized void readUnlock() { 
		if (--_reads == 0) notifyAll(); 
	}
	
	/**
	 * Obtain the read lock immediatelly or throw an exception if an 
	 * attempt to get the read lock would block this call
	 * 
	 * @throws BlockException if attempt to get the read lock would block this call
	 */
	public synchronized void readLockNoBlock() throws BlockException {
		if (_write || _waitWrites != 0) 
			throw new BlockException("block on read");
		readLock();
	}
	
	
	/** 
	 * Obtain a write lock 
	 * 
	 * @throws IntException if interrupted while waiting on the lock
	 */
	public synchronized void writeLock() { 
		while(_write || _reads != 0) { 
			++_waitWrites; 
			try { wait(); 
			} catch(InterruptedException ex) {
			throw new IntException();
			} finally { --_waitWrites; }
		}
		_write = true; 
	}
	
	/** 
	 * Revoke the write lock 
	 */
	public synchronized void writeUnlock() { 
		_write = false; 
		notifyAll(); 
	}
	
	/**
	 * Obtain the write lock immediatelly or throw an exception if an attempt 
	 * to get the write lock would block this call
	 * 
	 * @throws BlockException if attempt to get the write lock would block this 
	 * call
	 */
	public synchronized void writeLockNoBlock() throws BlockException {
		if (_write || _reads != 0) 
			throw new BlockException("block on write");
		writeLock();
	}
}
