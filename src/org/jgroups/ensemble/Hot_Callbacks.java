// $Id: Hot_Callbacks.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.ensemble;

public interface Hot_Callbacks {

    /**
    Called when Ensemble receives a Cast from your group
    */
    void receiveCast(Hot_GroupContext gctx, Object env, 
					Hot_Endpoint origin, Hot_Message msg);

    /**
    Called when Ensemble receives a point-to-point message for you
    */
    void receiveSend(Hot_GroupContext gctx, Object env, 
					Hot_Endpoint origin, Hot_Message msg);

    /**
    Called to update you with the new view
    */
    void acceptedView(Hot_GroupContext gctx, Object env, 
					Hot_ViewState viewState);

    /**
    Called to issue you a heartbeat
    */
    void heartbeat(Hot_GroupContext gctx, Object env, int rate);


    /**
    Called to let you know Ensemble is blocking
    */
    void block(Hot_GroupContext gctx, Object env);


    /**
    Called upon an Exit
    */
    void exit(Hot_GroupContext gctx, Object env);

}

