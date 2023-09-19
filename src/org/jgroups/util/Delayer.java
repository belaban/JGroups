package org.jgroups.util;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Collects actions by key and executes them - if a predicate is true - every N ms.
 * @author Bela Ban
 * @since  5.3.1
 */
public class Delayer<T> {
    protected long         timeout=2000; // ms
    protected Map<T,Entry> map=new ConcurrentHashMap<>();

    public Delayer(long timeout) {
        this.timeout=timeout;
    }

    public long       timeout()       {return timeout;}
    public Delayer<T> timeout(long t) {timeout=t; return this;}
    public int        size()          {return map.size();}
    public int        done()          {return (int)map.values().stream().filter(Entry::done).count();}

    public Delayer<T> add(T key, Predicate<T> pred, Consumer<Boolean> action) {
        map.computeIfAbsent(key, k -> new Entry(k, pred, action).run());
        return this;
    }

    public Delayer<T> clear() {
        map.values().forEach(e -> {
            if(e.f != null)
                e.f.cancel(true);
        });
        map.clear();
        return this;
    }

    @Override
    public String toString() {
        return String.format("%d entries (%d done)", size(), done());
    }

    protected class Entry {
        protected T                       key;
        protected Predicate<T>            pred;
        protected Consumer<Boolean>       action;
        protected CompletableFuture<Void> f;

        public Entry(T key, Predicate<T> pred, Consumer<Boolean> action) {
            this.key=key;
            this.pred=pred;
            this.action=action;
        }

        public boolean done() {
            return f.isDone();
        }

        public Entry run() {
            f=CompletableFuture.supplyAsync(() -> Util.waitUntilTrue(timeout, timeout / 10, () -> pred.test(key)))
              .thenAccept(success -> {
                  try {
                      action.accept(success);
                  }
                  finally {
                      map.remove(key);
                  }
              });
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s: done=%b", key, done());
        }
    }
}
