package org.jgroups.protocols.rules;

/**
 * @author Bela Ban
 * @since  3.3
 */
public class SampleRule extends AbstractRule {
    public String name() {
        return "SampleRule";
    }

    public String description() {
        return "Sample rule";
    }

    public boolean eval() {
        return true;
    }

    public String condition() {
        return "Dummy condition";
    }

    public void trigger() throws Throwable {
        System.out.println("SampleRule was triggered");
    }
}
