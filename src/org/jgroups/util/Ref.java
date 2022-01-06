package org.jgroups.util;

/**
 * Keeps a reference to another object
 * @author Bela Ban
 * @since  5.2
 */
public class Ref<T extends Object> {
    protected T obj;

    public Ref(T obj) {
        this.obj=obj;
    }

    public T get() {return obj;}

    public Ref<T> set(T obj) {
        this.obj=obj;
        return this;
    }

    public boolean isSet() {return obj != null;}

    public String toString() {
        return String.format("ref<%s>", obj);
    }
}
