
package org.jgroups.util;

/**
  * The Command patttern (see Gamma et al.). Implementations would provide their
  * own <code>execute</code> method.
  * @author Bela Ban
  */
public interface Command {
    boolean execute() throws Exception;
}
