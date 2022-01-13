package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.protocols.relay.SiteAddress;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;


/**
 * Sends a request to a single target destination
 *
 * @author Bela Ban
 */
public class UnicastRequest<T> extends Request<T> {
    protected final Address    target;


    public UnicastRequest(RequestCorrelator corr, Address target, RequestOptions options) {
        super(corr, options);
        this.target=target; // target is guaranteed to be non-null
    }

    @Override public void sendRequest(Message msg) throws Exception {
        try {
            corr.sendUnicastRequest(msg, options.mode() == ResponseMode.GET_NONE? null : this, this.options);
        }
        catch(Exception ex) {
            corrDone();
            throw ex;
        }
    }



    /* ---------------------- Interface RspCollector -------------------------- */
    /**
     * <b>Callback</b> (called by RequestCorrelator or Transport).
     * Adds a response to the response table. When all responses have been received, {@code execute()} returns.
     */
    public void receiveResponse(Object response_value, Address sender, boolean is_exception) {
        if(isDone())
            return;
        if(is_exception && response_value instanceof Throwable)
            completeExceptionally((Throwable)response_value);
        else
            complete((T)response_value);
        corrDone();
    }


    public void siteUnreachable(String site) {
        if(!(target instanceof SiteAddress) || !((SiteAddress)target).getSite().equals(site) || isDone())
            return;
        completeExceptionally(new UnreachableException(target));
        corrDone();
    }

    /**
     * If the target address is not a member of the new view, we'll mark the response as suspected and unblock
     * the caller of execute()
     */
    public void viewChange(View view, boolean handle_previous_subgroups) {
        if(view == null)
            return;

        if(view instanceof MergeView && handle_previous_subgroups) {
            // if target is not in a subview then we need to suspect it (https://issues.redhat.com/browse/JGRP-2575)
            for(View v: ((MergeView)view).getSubgroups()) {
                if(v.containsMember(target) && !v.containsMember(corr.local_addr)) {
                    completeExceptionally(new SuspectedException(target));
                    corrDone();
                    return;
                }
            }
        }
        // SiteAddresses are not checked as they might be in a different cluster
        if(!(target instanceof SiteAddress) && !view.containsMember(target) && !isDone()) {
            completeExceptionally(new SuspectedException(target));
            corrDone();
        }
    }

    public void transportClosed() {
        if(isDone())
            return;
        completeExceptionally(new IllegalStateException("transport was closed"));
        corrDone();
    }

    /* -------------------- End of Interface RspCollector ----------------------------------- */
    public T get() throws InterruptedException, ExecutionException {
        try {
            return super.get();
        }
        finally {
            corrDone();
        }
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return super.get(timeout, unit);
        }
        finally {
            corrDone();
        }
    }

    public T join() {
        return around(super::join);
    }

    public T getNow(T valueIfAbsent) {
        return around(() -> super.getNow(valueIfAbsent));
    }

    public T waitForCompletion(long timeout, TimeUnit unit) throws Exception {
        return getResult(() -> get(timeout, unit));
    }

    public T waitForCompletion() throws Exception {
        return getResult(this::get);
    }


    public String toString() {
        return String.format("%s, target=%s", super.toString(), target);
    }


    @GuardedBy("lock")
    public boolean responsesComplete() {
        return options.mode() == ResponseMode.GET_NONE || isDone();
    }

    protected T around(Supplier<T> supplier) {
        try {return supplier.get();}
        finally {corrDone();}
    }


    protected T getResult(Callable<T> supplier) throws Exception {
        try {
            return supplier.call();
        }
        catch(ExecutionException ex) {
            Throwable exception=ex.getCause();
            if(exception instanceof Error) throw (Error)exception;
            else if(exception instanceof RuntimeException) throw (RuntimeException)exception;
            else if(exception instanceof Exception) throw (Exception)exception;
            else throw new RuntimeException(exception);
        }
        finally {
            corrDone();
        }
    }
}
