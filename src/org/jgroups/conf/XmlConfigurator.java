// $Id: XmlConfigurator.java,v 1.1 2003/09/09 01:24:09 belaban Exp $

package org.jgroups.conf;

/**
 * Uses XML to configure a protocol stack
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
import org.jgroups.log.Trace;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;




public class XmlConfigurator implements ProtocolStackConfigurator
{
    private static boolean xml_debug = true;
    
    public static final String ATTR_NAME="name";
    public static final String ATTR_VALUE="value";
    public static final String ATTR_INHERIT="inherit";
    public static final String ELMT_PROT_OVERRIDE="protocol-override";
    public static final String ELMT_PROT="protocol";
    public static final String ELMT_PROT_NAME="protocol-name";
    public static final String ELMT_CLASS="class-name";
    public static final String ELMT_DESCRIPTION="description";
    public static final String ELMT_PROT_PARAMS="protocol-params";
    
    private ArrayList mProtocolStack = new ArrayList();
    private String mStackName;

    protected XmlConfigurator(String stackName,
                            ProtocolData[] protocols)
    {
        mStackName = stackName;
        for ( int i=0; i<protocols.length; i++ )
            mProtocolStack.add(protocols[i]);
        
    }
    
    protected XmlConfigurator(String stackName)
    {
        this(stackName,new ProtocolData[0]);
    }
    
    
    
    public static XmlConfigurator getInstance(URL url)
        throws java.io.IOException 
    {
        return getInstance(url.openStream());
    }
    
    public static XmlConfigurator getInstance(InputStream stream)
        throws java.io.IOException 
    {
        return parse(stream);
    }


    public static XmlConfigurator getInstance(Element el)
        throws java.io.IOException 
    {
        return parse(el);
    }

    
    public String getProtocolStackString()
    {
        StringBuffer buf =  new StringBuffer();
        Iterator it = mProtocolStack.iterator();
        while ( it.hasNext() )
        {
            ProtocolData d = (ProtocolData)it.next();
            buf.append(d.getProtocolString());
            if ( it.hasNext() ) buf.append(":"); 
        }//while
        return buf.toString();
    }
    
    public ProtocolData[] getProtocolStack() 
    {
        return (ProtocolData[]) mProtocolStack.toArray(new ProtocolData[mProtocolStack.size()]); 
    }

    public String getName() 
    { 
        return mStackName; 
    }
    
    public void override(ProtocolData data)
        throws IOException
    {
        int index = mProtocolStack.indexOf(data);
        if ( index < 0 ) throw new IOException("You can not override a protocol that doesn't exist");
        ProtocolData source = (ProtocolData)mProtocolStack.get(index);
        source.override(data.getParametersAsArray()); 
    }
    
    public void add(ProtocolData data)
    {
        mProtocolStack.add(data);
    }

    
    protected static XmlConfigurator parse(InputStream stream) throws java.io.IOException 
    {
        XmlConfigurator configurator = null;
        try
        {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(false); //for now
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setEntityResolver(new ClassPathEntityResolver());
            Document document = builder.parse( stream );
            Element root = (Element)document.getElementsByTagName("protocol-stack").item(0);
            root.normalize();
            //print("",new PrintWriter(System.out),root);
            String stackname = root.getAttribute(ATTR_NAME);
            String inherit = root.getAttribute(ATTR_INHERIT);
            boolean isinherited = (inherit != null && inherit.length()>0); 
            NodeList protocol_list = document.getElementsByTagName(isinherited?ELMT_PROT_OVERRIDE:ELMT_PROT);
            Vector v = new Vector(); 
            for ( int i=0; i<protocol_list.getLength(); i++ )
            {
                if ( protocol_list.item(i).getNodeType() == Node.ELEMENT_NODE )
                {
                    v.addElement(parseProtocolData(protocol_list.item(i)));
                }
            }
            ProtocolData[] protocols = new ProtocolData[v.size()];
            v.copyInto(protocols);
             
            if ( isinherited )
            {   
                URL inheritURL = new URL(inherit);
                configurator = XmlConfigurator.getInstance(inheritURL);
                for ( int i=0; i<protocols.length; i++ )
                    configurator.override(protocols[i]);
            }
            else
            {
                configurator = new XmlConfigurator(stackname,protocols);
            }//end if
            
        }
        catch ( Exception x )
        {
            if ( x instanceof java.io.IOException ) throw (java.io.IOException)x;
            else 
            {
                if ( xml_debug ) x.printStackTrace();
                String error = Trace.getStackTrace(x);
                Trace.error("XmlConfigurator",error);
                throw new java.io.IOException(x.getMessage());
            }
        }
        return configurator;
    }


    protected static XmlConfigurator parse(Element root) throws java.io.IOException 
    {
        XmlConfigurator configurator = null;
        try
        {
	    Document document=root.getOwnerDocument();
            //print("",new PrintWriter(System.out),root);
            String stackname = root.getAttribute(ATTR_NAME);
            String inherit = root.getAttribute(ATTR_INHERIT);
            boolean isinherited = (inherit != null && inherit.length()>0); 
            NodeList protocol_list = document.getElementsByTagName(isinherited?ELMT_PROT_OVERRIDE:ELMT_PROT);
            Vector v = new Vector(); 
            for ( int i=0; i<protocol_list.getLength(); i++ )
            {
                if ( protocol_list.item(i).getNodeType() == Node.ELEMENT_NODE )
                {
                    v.addElement(parseProtocolData(protocol_list.item(i)));
                }
            }
            ProtocolData[] protocols = new ProtocolData[v.size()];
            v.copyInto(protocols);
             
            if ( isinherited )
            {   
                URL inheritURL = new URL(inherit);
                configurator = XmlConfigurator.getInstance(inheritURL);
                for ( int i=0; i<protocols.length; i++ )
                    configurator.override(protocols[i]);
            }
            else
            {
                configurator = new XmlConfigurator(stackname,protocols);
            }//end if
            
        }
        catch ( Exception x )
        {
            if ( x instanceof java.io.IOException ) throw (java.io.IOException)x;
            else 
            {
                if ( xml_debug ) x.printStackTrace();
                String error = Trace.getStackTrace(x);
                Trace.error("XmlConfigurator",error);
                throw new java.io.IOException(x.getMessage());
            }
        }
        return configurator;
    }





    
    protected static ProtocolData parseProtocolData(Node protocol)
        throws java.io.IOException 
    {
        try
        {
            protocol.normalize();
            boolean isOverride = ELMT_PROT_OVERRIDE.equals(protocol.getNodeName());
            int pos = 0;
            NodeList children = protocol.getChildNodes();
            /**
             * there should be 4 Element Nodes if we are not overriding
             * 1. protocol-name
             * 2. description
             * 3. class-name
             * 4. protocol-params
             * 
             * If we are overriding we should have  
             * 1. protocol-name
             * 2. protocol-params
             */
            
            // 
            
            String name = null;
            String clazzname = null;
            String desc = null;
            ProtocolParameter[] plist = null;
            
            for ( int i=0; i<children.getLength(); i++ )
            {
                if ( children.item(i).getNodeType() == Node.ELEMENT_NODE )
                {
                    pos++;
                    if ( isOverride && (pos == 2) ) pos = 4;
                    switch ( pos )
                    {
                        case 1 : name = children.item(i).getFirstChild().getNodeValue(); break;
                        case 2 : desc =  children.item(i).getFirstChild().getNodeValue(); break;
                        case 3 : clazzname = children.item(i).getFirstChild().getNodeValue(); break;
                        case 4 : plist = parseProtocolParameters((Element)children.item(i)); break;    
                    }//switch
                }//end if
            }//for
            
            if ( isOverride ) 
                return new  ProtocolData(name,plist);
            else
                return new ProtocolData(name,desc,clazzname,plist);           
        }
        catch ( Exception x )
        {
            if ( x instanceof java.io.IOException ) throw (java.io.IOException)x;
            else 
            {
                
                if ( xml_debug ) x.printStackTrace();
                String error = Trace.getStackTrace(x);
                Trace.error("XmlConfigurator",error);
                throw new java.io.IOException(x.getMessage());
            }//end if           
        }//catch
    }
    
    protected static ProtocolParameter[] parseProtocolParameters(Element protparams)
        throws IOException
    {
        
        try
        {
            Vector v = new Vector();
            protparams.normalize();
            NodeList parameters = protparams.getChildNodes();
            for ( int i=0; i<parameters.getLength(); i++ )
            {
                if ( parameters.item(i).getNodeType() == Node.ELEMENT_NODE )
                {
                    String pname = parameters.item(i).getAttributes().getNamedItem(ATTR_NAME).getNodeValue();
                    String pvalue= parameters.item(i).getAttributes().getNamedItem(ATTR_VALUE).getNodeValue();
                    ProtocolParameter p = new ProtocolParameter(pname,pvalue);
                    v.addElement(p); 
                }//end if
            }//for
            ProtocolParameter[] result = new ProtocolParameter[v.size()];
            v.copyInto(result);
            return result;
        }
        catch ( Exception x )
        {
            if ( x instanceof java.io.IOException ) throw (java.io.IOException)x;
            else 
            {
                
                if ( xml_debug ) x.printStackTrace();
                String error = Trace.getStackTrace(x);
                Trace.error("XmlConfigurator",error);
                throw new java.io.IOException(x.getMessage());
            }//end if           
        }//catch
    }
    
    public static void main(String[] args) throws Exception
    {
	String          input_file=null, output=null;
	XmlConfigurator conf;

	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-url")) {
		input_file=args[++i];
		continue;
	    }
	    help();
	    return;
	}

        xml_debug = true;
	if(input_file != null) {
	    conf=XmlConfigurator.getInstance(new URL(input_file));
	    output=conf.getProtocolStackString();
	    output=replace(output, "org.jgroups.protocols.", "");
	    System.out.println("\n" + output);
	}
	else {
	    System.err.println("no input file given");
	}
    }


    public static String replace(String input, final String expr, String replacement) {
	StringBuffer sb=new StringBuffer();
	int          new_index=0, index=0, len=expr.length(), input_len=input.length();
	
	
	while(true) {
	    new_index=input.indexOf(expr, index);
	    if(new_index == -1) {
		sb.append(input.substring(index, input_len));
		break;
	    }
	    sb.append(input.substring(index, new_index));
	    sb.append(replacement);
	    index=new_index+len;
	}
	

	return sb.toString();
    }


    static void help() {
	System.out.println("XmlConfigurator [-help] [-url <URL> ]");
    }
}
