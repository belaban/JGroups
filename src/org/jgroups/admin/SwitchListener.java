package org.jgroups.admin;

/**
 * Listener which gets notifications about the progress of a switch
 * @author Bela Ban
 * @since  5.6.0
 */
public interface SwitchListener {
    void statusChange(double percentage, String msg);
}
