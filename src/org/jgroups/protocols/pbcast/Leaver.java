package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.util.Promise;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles the leaving of a member from a group. On leave(), a LEAVE-REQ is sent to the coordinator and the caller is
 * blocked until a LEAVE-RSP has been received.<br/>
 * If the coordinator changes before the response has been received, the LEAVE-REQ will be resent to the new
 * cordinator. If the coordinator is null, leave() returns.
 *
 * @author Bela Ban
 * @since  4.1.4
 */
public class Leaver {
    protected final GMS              gms;           // gms.local_mbr is the address of the member which is leaving
    protected final Log              log;
    protected final Promise<Address> leave_promise=new Promise<>();
    protected final AtomicBoolean    leaving=new AtomicBoolean(false);


    public Leaver(GMS gms) {
        this.gms=Objects.requireNonNull(gms);
        log=gms.getLog();
    }


    /**
     * Sends a LEAVE-REQ to the coordinator. Blocks the caller until a LEAVE-RSP has been received, or no coord is found.
     */
    public void leave() {
        Address coord, leaving_mbr;

        if(!leaving.compareAndSet(false, true)) {
            coord=gms.getCoord();
            if(coord == null) {
                log.trace("%s: last member in the group (coord); leaving now", gms.getAddress());
                leave_promise.setResult(gms.getAddress());
            }
            else {
                log.trace("%s: re-sending LEAVE request to %s", gms.getAddress(), coord);
                sendLeaveRequest(coord, gms.getAddress());
            }
            return;
        }

        try {
            int leave_attempts=0;
            leave_promise.reset(false);
            leaving_mbr=Objects.requireNonNull(gms.getAddress());
            coord=gms.getCoord();
            if(coord == null) {
                log.trace("%s: last member in the group (coord); leaving now", gms.getAddress());
                return;
            }
            log.trace("%s: sending LEAVE request to %s", gms.getAddress(), coord);
            long start=System.currentTimeMillis();
            sendLeaveRequest(coord, leaving_mbr);
            while(leaving.get()) {
                Address sender=leave_promise.getResult(gms.leave_timeout);
                long time=System.currentTimeMillis() - start;
                if(leave_promise.hasResult()) {
                    if(sender != null) {
                        boolean self=Objects.equals(sender, gms.getAddress());
                        log.trace("%s: got LEAVE response from %s in %d ms",
                                  gms.getAddress(), self? " self" : sender, time);
                    }
                    else
                        log.trace("%s: timed out waiting for LEAVE response from %s (after %d ms)",
                                  gms.getAddress(), coord, time);
                    break;
                }
                if(gms.max_leave_attempts > 0 && ++leave_attempts >= gms.max_leave_attempts) {
                    log.warn("%s: terminating after %d unsuccessful LEAVE attempts (waited %d ms): ",
                             gms.getAddress(), leave_attempts, time);
                    break;
                }
            }
        }
        finally {
            reset();
        }
    }


    public void handleLeaveResponse(Address sender) {
        leave_promise.setResult(Objects.requireNonNull(sender));
    }


    /**
     * Callback to notify Leaver that the coord changed. This will result in resending the LEAVE-REQ to the new
     * coordinator. If there's no new coord (e.g. because we're the last member),
     * {@link #leave()} (if there is one in progress) will return and unblock the caller.
     */
    public void coordChanged(Address new_coord) {
        if(!leaving.get() || new_coord == null)
            return;
        Address leaving_mbr=gms.getAddress();
        if(leaving_mbr == null) {
            log.error("local address is null, cannot re-send LEAVE request");
        }
        else {
            log.trace("%s: re-sending LEAVE request to %s", gms.getAddress(), new_coord);
            sendLeaveRequest(new_coord, leaving_mbr);
        }
    }


    /**
     * Interrupts and stops the current leave process (if one is in progress). No-op is no leave process is in progress.
     * This unblocks the caller of {@link #leave()} (if present).
     */
    public void reset() {
        if(leaving.compareAndSet(true, false))
            leave_promise.setResult(null);
    }


    protected void sendLeaveRequest(Address coord, Address leaving_mbr) {
        Message msg=new EmptyMessage(coord).setFlag(Message.Flag.OOB)
          .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.LEAVE_REQ, leaving_mbr));
        gms.getDownProtocol().down(msg);
    }
}
