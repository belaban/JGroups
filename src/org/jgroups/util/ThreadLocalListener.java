

package org.jgroups.util;

/**
 * Provides callback for use with a {@link ReusableThread}.
 */
public interface ThreadLocalListener {
    public void setThreadLocal();
    public void resetThreadLocal();
}
