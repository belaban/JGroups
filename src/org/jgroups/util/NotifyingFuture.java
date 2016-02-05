package org.jgroups.util;

import java.util.concurrent.Future;


/**
 * A sub-interface of a Future, that allows for listeners to be attached so that observers can be notified when the
 * future completes.
 * <p/>
 * See {@link FutureListener} for more details.
 * <p/>
 *
 * @author Manik Surtani
 * @since 2.9
 */
public interface NotifyingFuture<T> extends Future<T> {

    /**
     * Attaches a listener and returns the same future instance, to allow for 'building' futures.
     *
     * @param listener listener to attach
     * @return the same future instance
     */
    NotifyingFuture<T> setListener(FutureListener<T> listener);
}
