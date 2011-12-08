package org.jgroups.demos.dynamic;


/**
 * Abstract class for all commands
 * @author Bela Ban
 * @since 3.1
 */
public abstract class Command {
    protected DTest test;

    public DTest getTest() {
        return test;
    }

    public void setTest(DTest test) {
        this.test=test;
    }

    /**
     * Invokes a command against this command
     *
     * @param args
     * @return
     * @throws Exception
     */
    abstract public Object invoke(Object[] args) throws Exception;
    

    /** Returns a description of the command and its arguments */
    abstract public String help();
}
