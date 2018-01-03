package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.util.NameCache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simple protocol to inject an arbitrary view on one or more cluster nodes.</p>
 * INJECT_VIEW exposes a managed operation (injectView) capable of injecting a view by parsing the view state from a string.
 * The string format is A=A/B/C;B=B/C;C=C (where A,B,C are node names), this would inject view [A,B,C] with A as leader in node A,
 * view [B,C] with B as leader in node B and view [C] in node C.</p>
 * In order to leverage the injection on multiple nodes at once use a tool like probe.sh
 * (https://github.com/belaban/JGroups/blob/master/tests/other/org/jgroups/tests/Probe.java)
 * @author Andrea Tarocchi
 * @author Ugo Landini
 * @since 4.0.10
 * @see GMS
 */
@MBean(description="Protocol to inject an arbitrary view in nodes")
public class INJECT_VIEW extends Protocol {

    public static final String NODE_VIEWS_SEPARATOR = ";";
    public static final String VIEW_SEPARATOR = "=";
    public static final String NAMES_SEPARATOR = "/";

    @ManagedOperation(description="Inject a view (example of view string format: A=A/B/C;B=B/C;C=C)")
    public synchronized void injectView(String newView) {
        try {
            log.info("[INJECT_VIEW] Received request to inject view %s\n", newView);
            String[] perNode = newView.split(NODE_VIEWS_SEPARATOR);
            String thisNodeAddress = getProtocolStack().getChannel().getAddressAsString();

            for( String nodeView : perNode ){
                if( nodeView.startsWith(thisNodeAddress) ) {
                    log.info("[INJECT_VIEW] [channel: %s] Injecting a new view: %s\n", thisNodeAddress, nodeView);

                    long viewId = getProtocolStack().getChannel().getView().getViewId().getId()+1;
                    List<Address> nodes = new ArrayList<>();

                    for( String nodeName : nodeView.split(VIEW_SEPARATOR)[1].split(NAMES_SEPARATOR) ){
                        for( Map.Entry<Address, String> entry : NameCache.getContents().entrySet() ) {
                            if( nodeName.equals(entry.getValue()) ){
                                log.debug("[INJECT_VIEW] [channel: %s] Found name: <%s> for address: <%s>\n", entry.getValue(), entry.getKey().toString());
                                nodes.add( entry.getKey() );
                                break;
                            }
                        }
                    }

                    View view = new View( nodes.get(0), viewId, nodes);

                    GMS gms = getProtocolStack().findProtocol(GMS.class);
                    gms.installView(view);
                    log.info("[INJECT_VIEW] [channel: %s] Injection finished of view: %s\n", thisNodeAddress, nodeView);
                }
            }
        }
        catch(Exception e) {
            log.warn(e.getMessage(), e);
        }
    }
}
