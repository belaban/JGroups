package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Membership;
import org.jgroups.View;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements different merge policy (https://issues.jboss.org/browse/JGRP-1910). Might get merged with Merger
 * @author Bela Ban
 * @since  3.6.3
 */
public class Merger2 extends Merger {

    public Merger2(GMS gms) {
        super(gms);
    }



    /** Returns the address of the merge leader based on view ids */
    protected Address determineMergeLeader(Map<Address,View> views) {
        if(!gms.use_all_views_to_determine_merge_leaders)
            return super.determineMergeLeader(views);
        // we need the merge *coordinators* not merge participants because not everyone can lead a merge !
        Collection<Address> coords=Util.determineMergeCoords(views);
        if(coords.isEmpty()) {
            log.error("%s: unable to determine merge leader from %s; not starting a merge", gms.local_addr, views);
            return null;
        }
        return new Membership(coords).sort().elementAt(0); // establish a deterministic order, so that coords can elect leader
    }


    /**
     * Grabs the view coordinators
     */
    @Override
    protected Map<Address,Collection<Address>> determineMergeCoords(Map<Address,View> views) {
        Map<Address,Collection<Address>> retval=new HashMap<>();
        for(View view: views.values()) {
            Address coord=view.getCreator();
            Collection<Address> members=retval.get(coord);
            if(members == null)
                retval.put(coord, members=new ArrayList<>());
            for(Address mbr: view.getMembersRaw())
                if(!members.contains(mbr))
                    members.add(mbr);
        }

        // For the merge participants which are not coordinator, we simply add them, and the associated
        // membership list consists only of themselves
        Collection<Address> merge_participants=Util.determineMergeParticipants(views);
        merge_participants.removeAll(retval.keySet());
        for(Address merge_participant: merge_participants) {
            if(!retval.containsKey(merge_participant)) {
                Collection<Address> tmp=new ArrayList<>();
                tmp.add(merge_participant);
                retval.put(merge_participant, tmp);
            }
        }


        return retval;
    }
}
