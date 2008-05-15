package org.jgroups.util;

import org.jgroups.Address;

public class ThreadNamingPattern {
    private boolean includeClusterName=false;
    private boolean includeLocalAddress=false;
    private String clusterName;
    private Address address; 

    
    /**
     * Creates a default ThreadNamingPattern that does not rename threads.
     */
    public ThreadNamingPattern() {
    }

    /**
     * Creates a ThreadNamingPattern that renames threads according to specified pattern
     */
    public ThreadNamingPattern(String pattern) {
        setPattern(pattern);
    }

    public void setPattern(String pattern) {
        includeClusterName=pattern.contains("c");
        includeLocalAddress=pattern.contains("l");
    }
    
    public void setClusterName(String channelName){
        clusterName = channelName;       
    }
    
    public void setAddress(Address address){
        this.address = address;
    }

    public boolean isIncludeLocalAddress() {
        return includeLocalAddress;
    }

    public boolean isIncludeClusterName() {
        return includeClusterName;
    }

    public String renameThread(Thread runner) {
        String oldName = null;
        if (runner != null){
            oldName = runner.getName();
            renameThread(oldName,runner);
        }
        return oldName;           
    }

    public String renameThread(String base_name, Thread runner) {
        String oldName = null;
        if(runner != null){
            oldName = runner.getName();

            StringBuilder threadName = new StringBuilder();
            threadName.append(base_name);

            if(isIncludeClusterName()){
                if(threadName.length() > 0)
                    threadName.append(',');
                threadName.append(clusterName);
            }
            if(isIncludeLocalAddress()){
                if(threadName.length() > 0)
                    threadName.append(',');
                threadName.append(address);
            }

            runner.setName(threadName.toString());
        }
        return oldName;
    }      
}
