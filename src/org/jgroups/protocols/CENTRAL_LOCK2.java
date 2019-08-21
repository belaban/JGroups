package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.*;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;


/**
 * Implementation of a locking protocol which acquires locks by asking the coordinator.<br/>
 * Because the coordinator maintains all locks, no total ordering of requests is required.<br/>
 * CENTRAL_LOCK2 has all members send lock and unlock requests to the current coordinator. The coordinator has a queue
 * for incoming requests, and grants locks based on order of arrival.<br/>
 * Contrary to {@link CENTRAL_LOCK}, CENTRAL_LOCK2 has no members who act as backups for lock information. Instead,
 * when the coord leaves or on a merge, the new coordinator runs a <em>reconciliation</em> protocol in which it fetches
 * information from all members about acquired locks and pending lock and unlock requests, and then creates its lock
 * table accordingly. During this phase, all regular request handling is paused.<br/>
 * This protocol requires less traffic than {@link CENTRAL_LOCK} (each request also has to be sent to the backup(s)),
 * but introduces communication between the new coord and all members (and thus a small pause) on coord change.
 * <br/>
 * The JIRA issue is https://issues.jboss.org/browse/JGRP-2249.
 * @author Bela Ban
 * @since  4.0.13
 * @see Locking
 * @see CENTRAL_LOCK
 */
public class CENTRAL_LOCK2 extends Locking {
    @Property(description="By default, a lock owner is address:thread-id. If false, we only use the node's address. " +
      "See https://issues.jboss.org/browse/JGRP-1886 for details")
    protected boolean                                   use_thread_id_for_lock_owner=true;
    @Property(description="Max time (im ms) to wait for lock info responses from members in a lock reconciliation phase")
    protected long                                      lock_reconciliation_timeout=10_000;

    protected Address                                   coord;

    // collect information about held locks and pending lock requests from all members during a reconciliation round
    protected final ResponseCollector<LockInfoResponse> lock_info_responses=new ResponseCollector<>();

    // Queue to hold requests, typically only at the coordinator. Processed by RequestHandler
    protected final BlockingQueue<Request>              req_queue=new LinkedBlockingQueue<>();

    // Thread which processes requests in req-queue (running only on coord)
    protected final Runner                              req_handler;



    public CENTRAL_LOCK2() {
        req_handler=new Runner(new DefaultThreadFactory("lock-handler", true, true),
                               "lock-handler", this::processQueue, req_queue::clear);
    }

    @ManagedAttribute public boolean isCoord()                 {return Objects.equals(local_addr, coord);}
    @ManagedAttribute public String  getCoordinator()          {return coord != null? coord.toString() : "n/a";}
    @ManagedAttribute public boolean isRequestHandlerRunning() {return req_handler.isRunning();}
    @ManagedAttribute public int     requestQueueSize()        {return req_queue.size();}

    public void stop() {
        super.stop();
        req_handler.stop();
    }

    @Override
    public void handleView(View v) {
        Address old_coord=this.view != null? this.view.getCoord() : null;
        super.handleView(v);
        if(v.size() > 0) {
            coord=v.getCoord();
            log.debug("%s: coord=%s, is_coord=%b", local_addr, coord, isCoord());
        }

        if(Objects.equals(local_addr, coord)) {
            if(v instanceof MergeView || !Objects.equals(local_addr, old_coord)) {
                // I'm the new coord: run reconciliation to find all existing locks (and pending lock/unlock requests)
                runReconciliation();
                req_handler.start();
            }
        }
        else {
            if(Objects.equals(local_addr, old_coord)) {
                log.debug("%s: not coordinator anymore; stopping the request handler", local_addr);
                req_handler.stop(); // clears the req-queue
                server_locks.clear();
            }
        }
    }

    @Override
    protected void requestReceived(Request req) {
        if(req == null) return;
        switch(req.type) {

            // requests to be handled by the coord:
            case GRANT_LOCK:
            case RELEASE_LOCK:
            case CREATE_LOCK:
            case DELETE_LOCK:
            case COND_SIG:
            case COND_SIG_ALL:
            case LOCK_AWAIT:
            case DELETE_LOCK_AWAIT:
            case CREATE_AWAITER:
            case DELETE_AWAITER:
                req_queue.add(req);
                break;

            // requests/responses to be handled by clients
            case LOCK_GRANTED:
            case RELEASE_LOCK_OK:
            case LOCK_DENIED:
            case SIG_RET:
            case LOCK_INFO_REQ:
            case LOCK_INFO_RSP:
            case LOCK_REVOKED:
                if(log.isTraceEnabled())
                    log.trace("%s <-- %s: %s", local_addr, req.sender, req);
                handleRequest(req);
                break;

            default:
                log.error("%s: request of type %s not known", local_addr, req.type);
                break;
        }
    }

    protected void processQueue() {
        Request req=null;
        try {
            req=req_queue.take();
        }
        catch(InterruptedException e) {
        }
        try {
            if(req != null && log.isTraceEnabled())
                log.trace("%s <-- %s: %s", local_addr, req.sender, req);
            handleRequest(req);
        }
        catch(Throwable t) {
            log.error("%s: failed handling request %s: %s", local_addr, req, t);
        }
    }

    protected void handleLockInfoRequest(Address requester) {
        if(requester != null && !Objects.equals(coord, requester)) {
            log.trace("%s: changed coord from %s to %s as a result of getting a LOCK_INFO_REQ",
                     local_addr, coord, requester);
            coord=requester;
        }
        LockInfoResponse response=createLockInfoResponse();
        if(log.isTraceEnabled())
            log.trace("%s --> %s LOCK-INFO-RSP:\n%s", local_addr, requester, response.printDetails());
        send(requester, new Request(Type.LOCK_INFO_RSP).infoRsp(response));
    }

    @Override
    protected void handleLockInfoResponse(Address sender, Request rsp) {
        lock_info_responses.add(sender, rsp.info_rsp);
    }

    @Override
    protected void handleLockRevoked(Request rsp) {
        notifyLockRevoked(rsp.lock_name, rsp.owner);
    }

    /** Grabs information about locks held and pending lock/unlock requests from all members */
    @ManagedOperation(description="Runs the reconciliation protocol to fetch information about owned locks and pending " +
      "lock/unlock requests from each member to establish the server lock table. Only run by a coordinator.")
    public void runReconciliation() {
        if(!isCoord()) {
            log.warn("%s: reconciliation protocol is not run as I'm not the coordinator (%s is)",
                     local_addr, getCoordinator());
            return;
        }
        Request lock_info_req=new Request(Type.LOCK_INFO_REQ);
        Address[] mbrs=view.getMembersRaw();
        log.debug("%s: running reconciliation protocol on %d members", local_addr, mbrs != null? mbrs.length : 0);
        lock_info_responses.reset(mbrs);
        lock_info_responses.add(local_addr, createLockInfoResponse());
        log.trace("%s --> ALL: %s", local_addr, lock_info_req);

        // we cannot use a multicast as this may happen as a result of a MergeView and not everybody may have the view yet
        sendLockInfoRequestTo(Util.streamableToBuffer(lock_info_req), mbrs, local_addr);
        if(!lock_info_responses.waitForAllResponses(lock_reconciliation_timeout)) {
            List<Address> missing=lock_info_responses.getMissing();
            log.warn("%s: failed getting lock information from all members, missing responses: %d (from %s)",
                     local_addr, missing.size(), missing);
        }

        // 1. Add all existing locks to the server lock table
        Collection<LockInfoResponse> responses=lock_info_responses.getResults().values();
        responses.stream().filter(rsp -> rsp != null && rsp.existing_locks != null)
          .map(rsp -> rsp.existing_locks).flatMap(Collection::stream)
          .forEach(t -> {
              String lock_name=t.getVal1();
              Owner owner=t.getVal2();
              ServerLock srv_lock=new ServerLock(lock_name, owner);
              ServerLock ret=server_locks.putIfAbsent(lock_name, srv_lock);
              if(ret != null) {
                  if(!Objects.equals(owner, ret.owner)) {
                      log.warn("%s: lock %s requested by %s is already present: %s", local_addr, lock_name, owner, ret);
                      send(owner.getAddress(), new Request(Type.LOCK_REVOKED, lock_name, ret.owner, 0));
                  }
              }
              else {
                  notifyLockCreated(lock_name);
                  log.trace("%s: added lock %s", local_addr, lock_name);
              }
          });

        // 2. Process all pending requests
        responses.stream().filter(rsp -> rsp != null && rsp.pending_requests != null && !rsp.pending_requests.isEmpty())
          .map(rsp -> rsp.pending_requests).flatMap(Collection::stream)
          .forEach(req -> {
              try {
                  if(log.isTraceEnabled())
                      log.trace("%s: processing request %s", local_addr, req);
                  handleRequest(req);
              }
              catch(Throwable t) {
                  log.error("%s: failed handling request %s: %s", local_addr, req, t);
              }
          });
    }


    protected void sendLockInfoRequestTo(Buffer buf, Address[] mbrs, Address exclude) {
        Stream.of(mbrs).filter(m -> m != null && !Objects.equals(m, exclude)).forEach(dest -> {
            Message msg=new Message(dest, buf).putHeader(id, new LockingHeader());
            if(bypass_bundling)
                msg.setFlag(Message.Flag.DONT_BUNDLE);
            try {
                down_prot.down(msg);
            }
            catch(Throwable t) {
                log.error("%s: failed sending LOCK_INFO_REQ to %s: %s", local_addr, dest, t);
            }
        });
    }


    protected Owner getOwner() {
        return use_thread_id_for_lock_owner? super.getOwner(): new Owner(local_addr, -1);
    }

    protected void sendGrantLockRequest(String lock_name, int lock_id, Owner owner, long timeout, boolean is_trylock) {
        Address dest=coord;
        if(dest == null)
            throw new IllegalStateException("No coordinator available, cannot send GRANT-LOCK request");
        sendRequest(dest, Type.GRANT_LOCK, lock_name, lock_id, owner, timeout, is_trylock);
    }

    protected void sendReleaseLockRequest(String lock_name, int lock_id, Owner owner) {
        Address dest=coord;
        if(dest == null)
            throw new IllegalStateException("No coordinator available, cannot send RELEASE-LOCK request");
        sendRequest(dest, Type.RELEASE_LOCK, lock_name, lock_id, owner, 0, false);
    }

    @Override
    protected void sendAwaitConditionRequest(String lock_name, Owner owner) {
        sendRequest(coord, Type.LOCK_AWAIT, lock_name, owner, 0, false);
    }

    @Override
    protected void sendSignalConditionRequest(String lock_name, boolean all) {
        sendRequest(coord, all ? Type.COND_SIG_ALL : Type.COND_SIG, lock_name, null, 0, false);
    }
    
    @Override
    protected void sendDeleteAwaitConditionRequest(String lock_name, Owner owner) {
        sendRequest(coord, Type.DELETE_LOCK_AWAIT, lock_name, owner, 0, false);
    }


    protected LockInfoResponse createLockInfoResponse() {
        LockInfoResponse rsp=new LockInfoResponse();
        List<Tuple<String,Owner>> locks=client_lock_table.getLockInfo(); // successfully acquired locks
        for(Tuple<String,Owner> t: locks)
            rsp.add(t);

        List<Request> pending_reqs=client_lock_table.getPendingRequests(local_addr); // pending lock/unlock requests
        if(pending_reqs != null && !pending_reqs.isEmpty())
            rsp.pending_requests=pending_reqs;
        return rsp;
    }





}

