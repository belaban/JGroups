package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Handles merging. Called by CoordGmsImpl and ParticipantGmsImpl
 * @author Bela Ban
 */
public class Merger {
    protected final GMS                          gms;
    protected final Log                          log;

    protected final MergeTask                    merge_task=new MergeTask();

    /** For MERGE_REQ/MERGE_RSP correlation, contains MergeData elements */
    protected final ResponseCollector<MergeData> merge_rsps=new ResponseCollector<>();

    /** For GET_DIGEST / DIGEST_RSP correlation */
    protected final ResponseCollector<Digest>    digest_collector=new ResponseCollector<>();

    protected MergeId                            merge_id=null;

    protected final BoundedList<MergeId>         merge_id_history=new BoundedList<>(20);

    protected Future<?>                          merge_killer=null;



    public Merger(GMS gms) {
        this.gms=gms;
        this.log=gms.getLog();
    }

    public String               getMergeIdAsString()       {return merge_id != null? merge_id.toString() : null;}
    public String               getMergeIdHistory()        {return merge_id_history.toString();}
    public boolean              isMergeTaskRunning()       {return merge_task.isRunning();}
    public boolean              isMergeKillerTaskRunning() {return merge_killer != null && !merge_killer.isDone();}
    public synchronized MergeId getMergeId()               {return merge_id;} // only used for testing; do not use
    public synchronized boolean isMergeInProgress()        {return merge_id != null;}
    public synchronized boolean matchMergeId(MergeId id)   {return Util.match(this.merge_id, id);}


    public synchronized boolean setMergeId(MergeId expected, MergeId new_value) {
        boolean match=Util.match(this.merge_id, expected);
        if(match) {
            if(new_value != null && merge_id_history.contains(new_value))
                return false;
            else
                merge_id_history.add(new_value);
            this.merge_id=new_value;
            if(this.merge_id != null) {
                // Clears the view handler queue and discards all JOIN/LEAVE/MERGE requests until after the MERGE
                gms.getViewHandler().suspend();
                gms.getDownProtocol().down(new Event(Event.SUSPEND_STABLE, 20000));
                startMergeKiller();
            }
        }
        return match;
    }


    /**
     * Invoked upon receiving a MERGE event from the MERGE layer. Starts the merge protocol.
     * See description of protocol in DESIGN.
     * @param views A List of <em>different</em> views detected by the merge protocol, keyed by sender
     */
    public void merge(Map<Address, View> views) {
        if(views == null || views.isEmpty()) {
            log.warn("the views passed with the MERGE event were empty (or null); ignoring MERGE event");
            return;
        }

        if(View.sameViews(views.values())) {
            log.debug("MERGE event is ignored because of identical views: %s", Util.printListWithDelimiter(views.values(), ", "));
            return;
        }

        if(isMergeInProgress()) {
            log.trace("%s: merge is already running (merge_id=%s)", gms.local_addr, merge_id);
            return;
        }

        Address merge_leader=determineMergeLeader(views);
        if(merge_leader == null)
            return;

        if(merge_leader.equals(gms.local_addr)) {
            log.debug("%s: I will be the merge leader. Starting the merge task. Views: %s", gms.local_addr, views);
            merge_task.start(views);
        }
        else
            log.trace("%s: I'm not the merge leader, waiting for merge leader (%s) to start merge", gms.local_addr, merge_leader);
    }


    /**
     * Get the view and digest and send back both (MergeData) in the form of a MERGE_RSP to the sender.
     * If a merge is already in progress, send back a MergeData with the merge_rejected field set to true.
     * @param sender The address of the merge leader
     * @param merge_id The merge ID
     * @param mbrs The set of members from which we expect responses. Guaranteed to be non-null
     */
    public void handleMergeRequest(Address sender, MergeId merge_id, Collection<? extends Address> mbrs) {
        try {
            _handleMergeRequest(sender, merge_id, mbrs);
        }
        catch(Throwable t) {
            log.error("%s: failure handling the merge request: %s", gms.local_addr, t.getMessage());
            cancelMerge(merge_id);
            sendMergeRejectedResponse(sender, merge_id);
        }
    }

    public void handleMergeResponse(MergeData data, MergeId merge_id) {
        if(!matchMergeId(merge_id)) {
            log.trace("%s: this.merge_id (%s) is different from merge_id %s sent by %s as merge response, discarding it",
                      gms.local_addr, this.merge_id, merge_id, data.getSender());
            return;
        }
        merge_rsps.add(data.getSender(), data);
    }


    /**
     * If merge_id is not equal to this.merge_id then discard. Else cast the view/digest to all members of this group.
     */
    public void handleMergeView(final MergeData data, final MergeId merge_id) {
        if(!matchMergeId(merge_id)) {
            log.trace("%s: merge_ids (mine: %s, received: %s) don't match; merge view %s is discarded",
                      gms.local_addr, this.merge_id, merge_id, data.view.getViewId());
            return;
        }

        // only send to our *current* members, if we have A and B being merged (we are B), then we would *not*
        // want to block on a VIEW_ACK from A because A doesn't see us in the pre-merge view yet and discards the view
        List<Address> expected_acks=gms.members.getMembers();
        try {
            gms.castViewChangeAndSendJoinRsps(data.view, data.digest, expected_acks, null, null);
            // if we have flush in stack send ack back to merge coordinator
            if(gms.flushProtocolInStack) { //[JGRP-700] - FLUSH: flushing should span merge
                Message ack=new Message(data.getSender()).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
                  .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW_OK));
                gms.getDownProtocol().down(ack);
            }
        }
        finally {
            cancelMerge(merge_id);
        }
    }

    public void handleMergeCancelled(MergeId merge_id) {
        try {
            gms.stopFlush();
        }
        catch(Throwable t) {
            log.error(Util.getMessage("StopFlushFailed"), t.getMessage());
        }
        log.trace("%s: merge %s is cancelled", gms.local_addr, merge_id);
        cancelMerge(merge_id);
    }


    public void handleDigestResponse(Address sender, Digest digest) {
        digest_collector.add(sender, digest);
    }


    /**
     * Removes all members from a given view which don't have us in their view
     * (https://jira.jboss.org/browse/JGRP-1061). Example:
     * <pre>
     * A: AB
     * B: AB
     * C: ABC
     * </pre>
     * becomes
     * <pre>
     * A: AB
     * B: AB
     * C: C // A and B don't have C in their views
     * </pre>
     * @param map A map of members and their associated views
     */
    public static void sanitizeViews(Map<Address,View> map) {
        if(map == null)
            return;
        for(Map.Entry<Address,View> entry: map.entrySet()) {
            Address key=entry.getKey();
            List<Address> members=new ArrayList<>(entry.getValue().getMembers());
            boolean modified=false;
            for(Iterator<Address> it=members.iterator(); it.hasNext();) {
                Address val=it.next();
                if(val.equals(key)) // we can always talk to ourself !
                    continue;
                View view=map.get(val);
                final Collection<Address> tmp_mbrs=view != null? view.getMembers() : null;
                if(tmp_mbrs != null && !tmp_mbrs.contains(key)) {
                    it.remove();
                    modified=true;
                }
            }
            if(modified) {
                View old_view=entry.getValue();
                entry.setValue(new View(old_view.getViewId(), members));
            }
        }
    }





    /** Returns the address of the merge leader */
    protected Address determineMergeLeader(Map<Address,View> views) {
        // we need the merge *coordinators* not merge participants because not everyone can lead a merge !
        Collection<Address> coords=Util.determineActualMergeCoords(views);
        if(coords.isEmpty())
            coords=Util.determineMergeCoords(views); // https://issues.jboss.org/browse/JGRP-2092
        if(coords.isEmpty()) {
            log.error("%s: unable to determine merge leader from %s; not starting a merge", gms.local_addr, views);
            return null;
        }
        return new Membership(coords).sort().elementAt(0); // establish a deterministic order, so that coords can elect leader
    }

    /**
     * Needs to return a map of all subview coordinators and their views (as a collection of members). The merge policy
     * is defined in https://issues.jboss.org/browse/JGRP-1910
     */
    protected static Map<Address,Collection<Address>> determineMergeCoords(Map<Address,View> views) {
           Map<Address,Collection<Address>> retval=new HashMap<>();
           for(View view: views.values()) {
               Address coord=view.getCreator();
               Collection<Address> members=retval.computeIfAbsent(coord, k -> new ArrayList<>());
               for(Address mbr: view.getMembersRaw())
                   if(!members.contains(mbr))
                       members.add(mbr);
           }

           // For the merge participants which are not coordinator, we simply add them, and the associated
           // membership list consists only of themselves
           Collection<Address> merge_participants=Util.determineMergeParticipants(views);
           merge_participants.removeAll(retval.keySet());
           merge_participants.stream().filter(merge_participant -> !retval.containsKey(merge_participant))
             .forEach(merge_participant -> retval.put(merge_participant, Collections.singletonList(merge_participant)));
           return retval;
       }


    protected void _handleMergeRequest(Address sender, MergeId merge_id, Collection<? extends Address> mbrs) throws Exception {
        MergeId current_merge_id=this.merge_id;
        boolean success=matchMergeId(merge_id) || setMergeId(null, merge_id);
        if(!success) {
            log.trace("%s: merge %s is already in progress, received merge-id=%s", gms.local_addr, current_merge_id, merge_id);
            return;
        }

        /* Clears the view handler queue and discards all JOIN/LEAVE/MERGE requests until after the MERGE  */
        // gms.getViewHandler().suspend();
        log.trace("%s: got merge request from %s, merge_id=%s, mbrs=%s", gms.local_addr, sender, merge_id, mbrs);

        // merge the membership of the current view with mbrs
        List<Address> members=new ArrayList<>(mbrs != null? mbrs.size() : 32);
        if(mbrs != null) // didn't use a set because we didn't want to change the membership order at this time (although not incorrect)
            mbrs.stream().filter(mbr -> !members.contains(mbr)).forEach(members::add);

        // Now remove members that are not in our current view:
        members.retainAll(gms.view().getMembers());

        // Add myself if not present
        if(!members.contains(gms.local_addr))
            members.add(gms.local_addr);

        ViewId tmp_vid=gms.getViewId();
        if(tmp_vid == null)
            throw new Exception("view ID is null; cannot return merge response");
        View view=new View(tmp_vid, members);

        if(gms.flushProtocolInStack && !gms.startFlush(view)) // if flush is in stack, let this coord flush its subcluster
            throw new Exception("flush failed");

        // we still need to fetch digests from all members, and not just return our own digest (https://issues.jboss.org/browse/JGRP-948)
        Digest digest=fetchDigestsFromAllMembersInSubPartition(view, merge_id);
        if(digest == null || digest.capacity() == 0)
            throw new Exception("failed fetching digests from subpartition members; dropping merge response");
        
        sendMergeResponse(sender, view, digest, merge_id);
    }


    /** Send back a response containing view and digest to sender */
    protected void sendMergeResponse(Address sender, View view, Digest digest, MergeId merge_id) {
        Message msg=new Message(sender).setBuffer(GMS.marshal(view, digest)).setFlag(Message.Flag.OOB,Message.Flag.INTERNAL)
          .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP).mergeId(merge_id));
        gms.getDownProtocol().down(msg);
    }

    /**
     * Sends the new view and digest to all subgroup coordinators. Each coord will in turn broadcast the new view and
     * digest to all the members of its subgroup
     */
    protected void sendMergeView(Collection<Address> coords, MergeData combined_merge_data, MergeId merge_id) {
        if(coords == null || coords.isEmpty() || combined_merge_data == null)
            return;

        View view=combined_merge_data.view;
        Digest digest=combined_merge_data.digest;
        if(view == null || digest == null) {
            log.error(Util.getMessage("ViewOrDigestIsNullCannotSendConsolidatedMergeView/Digest"));
            return;
        }

        int size=0;
        if(gms.flushProtocolInStack) {
            gms.merge_ack_collector.reset(coords);
            size=gms.merge_ack_collector.size();
        }

        Event install_merge_view_evt=new Event(Event.INSTALL_MERGE_VIEW, view);
        gms.getUpProtocol().up(install_merge_view_evt);
        gms.getDownProtocol().down(install_merge_view_evt);

        long start=System.currentTimeMillis();
        for(Address coord: coords) {
            Message msg=new Message(coord).setBuffer(GMS.marshal(view, digest))
              .putHeader(gms.getId(),new GMS.GmsHeader(GMS.GmsHeader.INSTALL_MERGE_VIEW).mergeId(merge_id));
            gms.getDownProtocol().down(msg);
        }

        //[JGRP-700] - FLUSH: flushing should span merge; if flush is in stack, wait for acks from subview coordinators
        if(gms.flushProtocolInStack) {
            try {
                gms.merge_ack_collector.waitForAllAcks(gms.view_ack_collection_timeout);
                log.trace("%s: received all ACKs (%d) for merge view %s in %d ms",
                          gms.local_addr, size, view, (System.currentTimeMillis() - start));
            }
            catch(TimeoutException e) {
                log.warn("%s: failed to collect all ACKs (%d) for merge view %s after %d ms, missing ACKs from %s",
                         gms.local_addr, size, view, gms.view_ack_collection_timeout, gms.merge_ack_collector.printMissing());
            }
        }
    }

    protected void sendMergeRejectedResponse(Address sender, MergeId merge_id) {
        Message msg=new Message(sender).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.MERGE_RSP).mergeId(merge_id).mergeRejected(true));
        gms.getDownProtocol().down(msg);
    }

    protected void sendMergeCancelledMessage(Collection<Address> coords, MergeId merge_id) {
        if(coords == null || merge_id == null)
            return;
        for(Address coord: coords) {
            Message msg=new Message(coord).putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.CANCEL_MERGE).mergeId(merge_id));
            gms.getDownProtocol().down(msg);
        }
    }


    /**
     * Multicasts a GET_DIGEST_REQ to all members of this sub partition and waits for all responses
     * (GET_DIGEST_RSP) or N ms.
     */
    protected Digest fetchDigestsFromAllMembersInSubPartition(final View view, MergeId merge_id) {
        final List<Address> current_mbrs=view.getMembers();
        
        // Optimization: if we're the only member, we don't need to multicast the get-digest message
        if(current_mbrs == null || current_mbrs.size() == 1 && current_mbrs.get(0).equals(gms.local_addr))
            return new MutableDigest(view.getMembersRaw())
              .set((Digest)gms.getDownProtocol().down(new Event(Event.GET_DIGEST, gms.local_addr)));

        Message get_digest_req=new Message().setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.GET_DIGEST_REQ).mergeId(merge_id));

        long max_wait_time=gms.merge_timeout / 2; // gms.merge_timeout is guaranteed to be > 0, verified in init()
        digest_collector.reset(current_mbrs);

        gms.getDownProtocol().down(get_digest_req);

        // add my own digest first - the get_digest_req needs to be sent first *before* getting our own digest, so
        // we have that message in our digest !
        Digest digest=(Digest)gms.getDownProtocol().down(new Event(Event.GET_DIGEST, gms.local_addr));
        digest_collector.add(gms.local_addr, digest);
        digest_collector.waitForAllResponses(max_wait_time);
        if(log.isTraceEnabled()) {
            if(digest_collector.hasAllResponses())
                log.trace("%s: fetched all digests for %s", gms.local_addr, current_mbrs);
            else
                log.trace("%s: fetched incomplete digests (after timeout of %d) ms for %s",
                          gms.local_addr, max_wait_time, current_mbrs);
        }

        List<Address> valid_rsps=new ArrayList<>(current_mbrs);
        valid_rsps.removeAll(digest_collector.getMissing());

        Address[] tmp=new Address[valid_rsps.size()];
        valid_rsps.toArray(tmp);
        MutableDigest retval=new MutableDigest(tmp);
        Map<Address,Digest> responses=new HashMap<>(digest_collector.getResults());
        responses.values().forEach(retval::set);
        return retval;
    }

    /**
     * Fetches the digests from all members and installs them again. Used only for diagnosis and support; don't
     * use this otherwise !
     */
    protected void fixDigests() {
        Digest digest=fetchDigestsFromAllMembersInSubPartition(gms.view, null);
        Message msg=new Message().putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.INSTALL_DIGEST))
          .setBuffer(GMS.marshal(null, digest));
        gms.getDownProtocol().down(msg);
    }


    protected void stop() {
        merge_task.stop();
    }


    protected synchronized void cancelMerge(MergeId id) {
        if(setMergeId(id, null)) {
            merge_task.stop();
            stopMergeKiller();
            merge_rsps.reset();
            gms.getViewHandler().resume();
            gms.getDownProtocol().down(new Event(Event.RESUME_STABLE));
        }
    }



    protected synchronized void forceCancelMerge() {
        if(this.merge_id != null)
            cancelMerge(this.merge_id);
    }


    protected synchronized void startMergeKiller() {
        if(merge_killer == null || merge_killer.isDone()) {
            MergeKiller task=new MergeKiller(this.merge_id);
            merge_killer=gms.timer.schedule(task, gms.merge_timeout * 2, TimeUnit.MILLISECONDS, false);
        }
    }

    protected synchronized void stopMergeKiller() {
        if(merge_killer != null) {
            merge_killer.cancel(false);
            merge_killer=null;
        }
    }






    /**
     * Starts the merge protocol (only run by the merge leader). Essentially sends a MERGE_REQ to all
     * coordinators of all subgroups found. Each coord receives its digest and view and returns it.
     * The leader then computes the digest and view for the new group from the return values. Finally, it
     * sends this merged view/digest to all subgroup coordinators; each coordinator will install it in their
     * subgroup.
     */
    protected class MergeTask implements Runnable {
        protected Thread thread=null;

        /** List of all subpartition coordinators and their members */
        protected final ConcurrentMap<Address,Collection<Address>> coords=Util.createConcurrentMap(8, 0.75f, 8);
        protected final Set<View>                                  subviews=new HashSet<>();


        /**
         * @param views Guaranteed to be non-null and to have >= 2 members, or else this thread would not be started
         */
        public synchronized void start(Map<Address, View> views) {
            if(thread != null && thread.isAlive()) // the merge thread is already running
                return;

            this.coords.clear();
            this.subviews.clear();
            subviews.addAll(views.values());

            // now remove all members which don't have us in their view, so RPCs won't block (e.g. FLUSH)
            // https://jira.jboss.org/browse/JGRP-1061
            sanitizeViews(views);

            Map<Address,Collection<Address>> tmp_coords=determineMergeCoords(views);
            this.coords.putAll(tmp_coords);

            thread=gms.getThreadFactory().newThread(this, "MergeTask");
            thread.setDaemon(true);
            thread.start();
        }


        public synchronized void stop() {
            Thread tmp=thread;
            if(thread != null && thread.isAlive())
                tmp.interrupt();
            thread=null;
        }


        public synchronized boolean isRunning() {
            return thread != null && thread.isAlive();
        }


        public void run() {
            // 1. Generate merge_id
            final MergeId new_merge_id=MergeId.create(gms.local_addr);
            final Collection<Address> coordsCopy=new ArrayList<>(coords.keySet());

            long start=System.currentTimeMillis();

            try {
               _run(new_merge_id, coordsCopy); // might remove members from coordsCopy
            }
            catch(Throwable ex) {
                if(ex instanceof Error || ex instanceof RuntimeException)
                    log.warn(gms.local_addr + ": merge is cancelled", ex);
                else
                    log.warn("%s: merge is cancelled: %s", gms.local_addr, ex.getMessage());
                sendMergeCancelledMessage(coordsCopy, new_merge_id);
                cancelMerge(new_merge_id); // the message above cancels the merge, too, but this is a 2nd line of defense
            }
            finally {
                /* 5. if flush is in stack stop the flush for entire cluster [JGRP-700] - FLUSH: flushing should span merge */
                if(gms.flushProtocolInStack)
                    gms.stopFlush();
                thread=null;
            }
            long diff=System.currentTimeMillis() - start;
            log.debug("%s: merge %s took %d ms", gms.local_addr, new_merge_id, diff);
        }

        /** Runs the merge protocol as a leader */
        protected void _run(MergeId new_merge_id, final Collection<Address> coordsCopy) throws Exception {
            boolean success=setMergeId(null, new_merge_id);
            if(!success) {
                log.warn("%s: failed to set my own merge_id (%s) to %s", gms.local_addr, merge_id, new_merge_id);
                return;
            }

            log.debug("%s: merge task %s started with %d participants", gms.local_addr, merge_id, coords.keySet().size());

            // Fetch the current views and digests from all subgroup coordinators into merge_rsps
            success=getMergeDataFromSubgroupCoordinators(coords, new_merge_id, gms.merge_timeout);
            List<Address> missing=null;
            if(!success) {
                missing=merge_rsps.getMissing();
                log.debug("%s: merge leader %s did not get responses from all %d partition coordinators; " +
                            "missing responses from %d members, removing them from the merge",
                          gms.local_addr, gms.local_addr, coords.keySet().size(), missing.size());
                merge_rsps.remove(missing);
            }

            // Remove null or rejected merge responses from merge_rsp and coords (so we'll send the new view
            // only to members who accepted the merge request)
            if(missing != null && !missing.isEmpty()) {
                coords.keySet().removeAll(missing);
                coordsCopy.removeAll(missing);
            }

            removeRejectedMergeRequests(coords.keySet());
            if(merge_rsps.size() == 0)
                throw new Exception("did not get any merge responses from partition coordinators");

            if(!coords.keySet().contains(gms.local_addr)) // another member might have invoked a merge req on us before we got there...
                throw new Exception("merge leader rejected merge request");

            // Combine all views and digests into 1 View/1 Digest
            List<MergeData> merge_data=new ArrayList<>(merge_rsps.getResults().values());
            MergeData combined_merge_data=consolidateMergeData(merge_data, new ArrayList<>(subviews));
            if(combined_merge_data == null)
                throw new Exception("could not consolidate merge");

            // send the new view/digest to all coords (including myself). On reception, they will
            // install the digest and view in all of their subgroup members
            log.debug("%s: installing merge view %s (%d members) in %d coords",
                      gms.local_addr, combined_merge_data.view.getViewId(), combined_merge_data.view.size(), coords.keySet().size());
            sendMergeView(coords.keySet(), combined_merge_data, new_merge_id);
        }



        /**
         * Sends a MERGE_REQ to all coords and populates a list of MergeData (in merge_rsps). Returns after coords.size()
         * response have been received, or timeout msecs have elapsed (whichever is first).<p>
         * If a subgroup coordinator rejects the MERGE_REQ (e.g. because of participation in a different merge),
         * <em>that member will be removed from coords !</em>
         * @param coords A map of coordinatgor addresses and associated membership lists
         * @param new_merge_id The new merge id
         * @param timeout Max number of msecs to wait for the merge responses from the subgroup coords
         */
        protected boolean getMergeDataFromSubgroupCoordinators(Map<Address,Collection<Address>> coords, MergeId new_merge_id, long timeout) {
            boolean gotAllResponses;
            long start=System.currentTimeMillis();
            merge_rsps.reset(coords.keySet());
            log.trace("%s: sending MERGE_REQ to %s", gms.local_addr, coords.keySet());

            for(Map.Entry<Address,Collection<Address>> entry: coords.entrySet()) {
                Address coord=entry.getKey();
                Collection<Address> mbrs=entry.getValue();
                Message msg=new Message(coord).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
                  .putHeader(gms.getId(), new GMS.GmsHeader(GMS.GmsHeader.MERGE_REQ).mbr(gms.local_addr).mergeId(new_merge_id))
                  .setBuffer(GMS.marshal(mbrs));
                gms.getDownProtocol().down(msg);
            }

            // wait until num_rsps_expected >= num_rsps or timeout elapsed
            merge_rsps.waitForAllResponses(timeout);
            gotAllResponses=merge_rsps.hasAllResponses();
            long time=System.currentTimeMillis() - start;
            log.trace("%s: collected %d merge response(s) in %d ms", gms.local_addr, merge_rsps.numberOfValidResponses(), time);
            return gotAllResponses;
        }


        /** Removed rejected merge requests from merge_rsps and coords. This method has a lock on merge_rsps */
        protected void removeRejectedMergeRequests(Collection<Address> coords) {
            int num_removed=0;
            for(Iterator<Map.Entry<Address,MergeData>> it=merge_rsps.getResults().entrySet().iterator(); it.hasNext();) {
                Map.Entry<Address,MergeData> entry=it.next();
                MergeData data=entry.getValue();
                if(data.merge_rejected) {
                    if(data.getSender() != null)
                        coords.remove(data.getSender());
                    it.remove();
                    num_removed++;
                }
            }
            if(num_removed > 0)
                log.trace("%s: removed %d rejected merge responses", gms.local_addr, num_removed);
        }

        /**
         * Merge all MergeData. All MergeData elements should be disjunct (both views and digests). However,
         * this method is prepared to resolve duplicate entries (for the same member). The resolution strategy for
         * views is to merge only 1 of the duplicate members. Resolution strategy for digests is to take the higher
         * seqnos for duplicate digests.<p>
         * After merging all members into a Membership and subsequent sorting, the first member of the sorted membership
         * will be the new coordinator. This method has a lock on merge_rsps.
         * @param merge_rsps A list of MergeData items. Elements with merge_rejected=true were removed before. Is guaranteed
         *          not to be null and to contain at least 1 member
         * @param subviews Contains a list of Views, each View is a subgroup
         */
        protected MergeData consolidateMergeData(List<MergeData> merge_rsps, List<View> subviews) {
            long                            logical_time=0; // for new_vid
            Collection<Collection<Address>> sub_mbrships=new ArrayList<>();
            Set<Address>                    digest_membership=new HashSet<>(); // members as seen by the digests

            for(MergeData tmp_data: merge_rsps) {
                View tmp_view=tmp_data.getView();
                if(tmp_view != null) {
                    ViewId tmp_vid=tmp_view.getViewId();
                    if(tmp_vid != null)
                        logical_time=Math.max(logical_time, tmp_vid.getId()); // compute the new view id (max of all vids +1)

                    // merge all membership lists into one (prevent duplicates)
                    sub_mbrships.add(new ArrayList<>(tmp_view.getMembers()));
                }
                Digest digest=tmp_data.getDigest();
                if(digest != null)
                    for(Digest.Entry entry: digest)
                        digest_membership.add(entry.getMember());
            }

            // remove all members from the new view for which we didn't get a digest, e.g. new view={A,B,C,D,E,F},
            // digest={A,C,D,F} --> new view={A,C,D,F}, digest={A,C,D,F}
            if(!digest_membership.isEmpty())
                for(Collection<Address> coll: sub_mbrships)
                    coll.retainAll(digest_membership);

            List<Address> merged_mbrs=gms.computeNewMembership(sub_mbrships);

            // remove all members from the (future) MergeView that are not in the digest
            // Usually not needed, but a second line of defense in case the membership change policy above
            // computed the new view incorrectly, e.g. not removing dupes
            Set<Address> all_members=new HashSet<>();
            sub_mbrships.forEach(all_members::addAll);
            merged_mbrs.retainAll(all_members);

            // the new coordinator is the first member of the consolidated & sorted membership list
            Address new_coord=merged_mbrs.isEmpty()? null : merged_mbrs.get(0);
            if(new_coord == null)
                return null;

            // Remove views from subviews whose creator are not in the new membership
            for(Iterator<View> it=subviews.iterator(); it.hasNext();) {
                View v=it.next();
                logical_time=Math.max(logical_time, v.getViewId().getId());
                Address creator=v.getCreator();
                if(creator != null && !merged_mbrs.contains(creator))
                    it.remove();
            }

            // determine the new view; logical_time should be the highest view ID seen up to now plus 1
            MergeView new_view=new MergeView(new_coord, logical_time + 1, merged_mbrs, subviews);

            // determine the new digest
            MutableDigest new_digest=consolidateDigests(new_view, merge_rsps);
            if(new_digest == null || !new_digest.allSet())
                return null;

            log.trace("%s: consolidated view=%s\nconsolidated digest=%s", gms.local_addr, new_view, new_digest);
            return new MergeData(gms.local_addr, new_view, new_digest);
        }

        /**
         * Merge all digests into one. For each sender, the new value is max(highest_delivered),
         * max(highest_received). This method has a lock on merge_rsps
         */
        protected MutableDigest consolidateDigests(final View new_view, final List<MergeData> merge_rsps) {
            MutableDigest retval=new MutableDigest(new_view.getMembersRaw());
            for(MergeData data: merge_rsps) {
                Digest tmp_digest=data.getDigest();
                if(tmp_digest != null)
                    retval.merge(tmp_digest);
            }
            return retval;
        }
    }


    protected class MergeKiller implements Runnable {
        protected final MergeId my_merge_id;

        MergeKiller(MergeId my_merge_id) {
            this.my_merge_id=my_merge_id;
        }

        public void run() {
            cancelMerge(my_merge_id);
        }

        public String toString() {
            return Merger.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }

}
