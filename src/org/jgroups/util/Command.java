// $Id: Command.java,v 1.3 2006/04/05 05:33:57 belaban Exp $

package org.jgroups.util;

/**
  * The Command patttern (see Gamma et al.). Implementations would provide their
  * own <code>execute</code> method.
  * @author Bela Ban
  */
public interface Command {
    boolean execute() throws Exception;
}
