// $Id: Command.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;

/**
  * The Command patttern (se Gamma et al.). Implementations would provide their
  * own <code>execute</code> method.
  * @author Bela Ban
  */
public interface Command {
    boolean execute();
}
