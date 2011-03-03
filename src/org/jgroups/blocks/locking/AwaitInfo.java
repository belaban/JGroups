package org.jgroups.blocks.locking;


public class AwaitInfo {
    protected final String   name;
    protected final boolean  all;
    
    public AwaitInfo(String name, boolean all) {
        this.name=name;
        this.all=all;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Returns whether is all.
     */
    public boolean isAll() {
        return all;
    }
    
    public String toString() {
        return name + ", awaitAll=" + all;
    }
}
