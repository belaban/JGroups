package org.jgroups.protocols.rules;

import org.jgroups.logging.Log;

/**
 * A rule with a condition and (optional) action. When a rule is run, the condition is checked with {@link #eval()} and
 * - if true - the action is triggered with {@link #trigger()}.
 * @author Bela Ban
 * @since  3.3
 */
public abstract class AbstractRule implements Runnable {
    protected SUPERVISOR sv;  // set when rule is installed
    protected Log        log;         // set when rule is installed

    public AbstractRule supervisor(SUPERVISOR sv) {this.sv=sv;   return this;}
    public AbstractRule log(Log log)              {this.log=log; return this;}

    /** Returns the name of the rule. Should be unique if a rule needs to be uninstalled */
    public abstract String name();

    /** Describes what the rules does */
    public abstract String description();

    /** Called when rule is installed */
    public void init() {}

    /** Called when rule is uninstalled */
    public void destroy() {}

    /** Evaluates the condition. If true, the rule is triggered. If true, the next execution of {@link #condition()}
     * should return a non-null string */
    public abstract boolean eval();

    /** Returns a description of the condition that led to {@link #eval()} returning true */
    public abstract String condition();

    /** The action of the rule. Triggered if {@link #eval()} returned true */
    public abstract void trigger() throws Throwable;

    public void run() {
        if(!eval())
            return;
        try {
            String condition=condition();
            if(condition == null)
                condition="executed rule " + name();
            sv.addCondition(condition);
            if(log.isTraceEnabled())
                log.trace(sv.getLocalAddress() + ": executing rule " + name());
            trigger();
        }
        catch(Throwable t) {
            log.error(sv.getLocalAddress() + ": failed executiong rule " + name(), t);
        }
    }
}
