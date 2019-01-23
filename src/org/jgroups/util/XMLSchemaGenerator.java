package org.jgroups.util;

import org.jgroups.Version;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.XmlAttribute;
import org.jgroups.annotations.XmlElement;
import org.jgroups.annotations.XmlInclude;
import org.jgroups.stack.Protocol;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.*;

/**
 * Iterates over all concrete Protocol classes and creates XML schema used for validation of configuration files.
 * 
 * https://jira.jboss.org/jira/browse/JGRP-448
 * 
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * 
 */
public class XMLSchemaGenerator {

    protected static final String PROT_PACKAGE="org.jgroups.protocols";

    protected static final String[] PACKAGES={"", "pbcast", "tom", "relay", "rules", "dns"};

    static {
        System.setProperty("java.awt.headless", "true");
    }

    public static void main(String[] args) {

        String outputDir = "./";

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("-o".equals(arg)) {
                outputDir = args[++i];
            } else {
                System.out.println("XMLSchemaGenerator -o <path to newly created xsd schema file>");
                return;
            }
        }

        String version = Version.major + "." + Version.minor;
        File f = new File(outputDir, "jgroups-" + version + ".xsd");
        try {
            FileWriter fw = new FileWriter(f, false);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            DOMImplementation impl = builder.getDOMImplementation();
            Document xmldoc = impl.createDocument("http://www.w3.org/2001/XMLSchema", "xs:schema", null);
            xmldoc.getDocumentElement().setAttribute("targetNamespace", "urn:org:jgroups");
            xmldoc.getDocumentElement().setAttribute("xmlns:tns", "urn:org:jgroups");
            xmldoc.getDocumentElement().setAttribute("elementFormDefault", "qualified");
            xmldoc.getDocumentElement().setAttribute("attributeFormDefault", "unqualified");
            xmldoc.getDocumentElement().setAttribute("version", version);

            Element complexType = xmldoc.createElement("xs:complexType");
            complexType.setAttribute("name", "ConfigType");
            xmldoc.getDocumentElement().appendChild(complexType);

            Element allType = xmldoc.createElement("xs:choice");
            allType.setAttribute("maxOccurs", "unbounded");
            complexType.appendChild(allType);

            generateProtocolSchema(xmldoc, allType, PACKAGES);

            Element xsElement = xmldoc.createElement("xs:element");
            xsElement.setAttribute("name", "config");
            xsElement.setAttribute("type", "tns:ConfigType");
            xmldoc.getDocumentElement().appendChild(xsElement);

            DOMSource domSource = new DOMSource(xmldoc);
            StreamResult streamResult = new StreamResult(fw);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer serializer = tf.newTransformer();
            serializer.setOutputProperty(OutputKeys.METHOD, "xml");
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            serializer.transform(domSource, streamResult);

            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void generateProtocolSchema(Document xmldoc, Element parent, String... suffixes) throws Exception {
        for(String suffix: suffixes) {
            String package_name=PROT_PACKAGE + (suffix == null || suffix.isEmpty()? "" : "." + suffix);
            Set<Class<?>> classes=getClasses(Protocol.class, package_name);
            List<Class<?>> sortedClasses = new LinkedList<>(classes);
            Collections.sort(sortedClasses, Comparator.comparing(Class::getCanonicalName));
            for (Class<?> clazz : sortedClasses)
                classToXML(xmldoc, parent, clazz, package_name);
        }
    }


    private static Set<Class<?>> getClasses(Class<?> assignableFrom, String packageName)
      throws ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Set<Class<?>> classes = new HashSet<>();
        String path = packageName.replace('.', '/');
        URL resource = loader.getResource(path);
        if (resource != null) {
            String filePath = resource.getFile();
            if (filePath != null && new File(filePath).isDirectory()) {
                for (String file : new File(filePath).list()) {
                    if (file.endsWith(".class")) {
                        String name = packageName + '.' + file.substring(0, file.indexOf(".class"));
                        Class<?> clazz = Class.forName(name);

                        int mods=clazz.getModifiers();
                        boolean isConcreteClass=!Modifier.isAbstract(mods);
                        boolean is_public=Modifier.isPublic(mods);
                        boolean generate=is_public && isConcreteClass && !clazz.isAnonymousClass();

                        if (assignableFrom.isAssignableFrom(clazz) && generate)
                            classes.add(clazz);
                    }
                }
            }
        }
        return classes;
    }

    private static void classToXML(Document xmldoc, Element parent, Class<?> clazz,
                                   String preAppendToSimpleClassName) throws Exception {
        XmlInclude incl=Util.getAnnotation(clazz, XmlInclude.class);
        if(incl != null) {
            String[] schemas=incl.schema();
            for (String schema : schemas) {
                Element incl_el = xmldoc.createElement(incl.type() == XmlInclude.Type.IMPORT ? "xs:import" : "xs:include");
                if (!incl.namespace().isEmpty())
                    incl_el.setAttribute("namespace", incl.namespace());
                incl_el.setAttribute("schemaLocation", schema);

                Node first_child = xmldoc.getDocumentElement().getFirstChild();
                if (first_child == null)
                    xmldoc.getDocumentElement().appendChild(incl_el);
                else
                    xmldoc.getDocumentElement().insertBefore(incl_el, first_child);
            }
            if(!incl.alias().isEmpty())
                xmldoc.getDocumentElement().setAttribute("xmlns:" + incl.alias(), incl.namespace());
        }

        parent.appendChild(createXMLTree(xmldoc, clazz, preAppendToSimpleClassName));
    }

    private static Element createXMLTree(final Document xmldoc, Class<?> clazz, String pkgname) throws Exception {

        Element classElement = xmldoc.createElement("xs:element");
        String elementName = pkgname + "." + clazz.getSimpleName();
        if(elementName.isEmpty()) {
            throw new IllegalArgumentException("Cannot create empty attribute name for element xs:element, class is " + clazz);
        }

        elementName=elementName.replace(PROT_PACKAGE + ".", "");

        classElement.setAttribute("name",elementName);

        final Element complexType = xmldoc.createElement("xs:complexType");
        classElement.appendChild(complexType);

        // the protocol has its own subtree
        XmlElement el=Util.getAnnotation(clazz, XmlElement.class);
        if(el != null) {
            Element choice=xmldoc.createElement("xs:choice");
            choice.setAttribute("minOccurs", "0");
            choice.setAttribute("maxOccurs", "unbounded");
            complexType.appendChild(choice);
            Element tmp=xmldoc.createElement("xs:element");
            tmp.setAttribute("name", el.name());
            tmp.setAttribute("type", el.type());
            choice.appendChild(tmp);
        }

        Map<String, DelayingElementWriter> sortedElements =new TreeMap<>();

        XmlAttribute xml_attr=Util.getAnnotation(clazz, XmlAttribute.class);
        if(xml_attr != null) {
            String[] attrs=xml_attr.attrs();
            if(attrs.length > 0) {
                Set<String> set=new HashSet<>(Arrays.asList(attrs)); // to weed out dupes
                for(final String attr: set) {
                    sortedElements.put(attr, () -> {
                        Element attributeElement = xmldoc.createElement("xs:attribute");
                        attributeElement.setAttribute("name", attr);
                        attributeElement.setAttribute("type", "xs:string");
                        complexType.appendChild(attributeElement);
                    });
                }
            }
        }


        // iterate fields
        for (Class<?> clazzInLoop = clazz; clazzInLoop != null; clazzInLoop = clazzInLoop.getSuperclass()) {
            Field[] fields = clazzInLoop.getDeclaredFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(Property.class)) {
                    final String property;
                    final Property r = field.getAnnotation(Property.class);
                    boolean annotationRedefinesName = !r.name().isEmpty() && r.deprecatedMessage().isEmpty();
                    if (annotationRedefinesName) {
                        property = r.name();
                    } else {
                        property = field.getName();
                    }
                    if(property == null || property.isEmpty()) {
                        throw new IllegalArgumentException("Cannot create empty attribute name for element xs:attribute, field is " + field);
                    }
                    sortedElements.put(property, () -> {
                        Element attributeElement = xmldoc.createElement("xs:attribute");
                        attributeElement.setAttribute("name", property);

                        // Agreement with Bela Ban on Jan-20-2009 (Go Obama!!!) to treat all types as
                        // xs:string since we do not know where users are going to use
                        // replacement tokens in configuration files. Therefore, the type becomes
                        // indeterminate.
                        attributeElement.setAttribute("type", "xs:string");
                        complexType.appendChild(attributeElement);

                        Element annotationElement = xmldoc.createElement("xs:annotation");
                        attributeElement.appendChild(annotationElement);

                        Element documentationElement = xmldoc.createElement("xs:documentation");
                        documentationElement.setTextContent(r.description());
                        annotationElement.appendChild(documentationElement);
                    });
                }
            }
        }

        // iterate methods
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Property.class)) {

                final Property annotation = method.getAnnotation(Property.class);
                final String name;
                if (annotation.name().length() < 1) {
                    name = Util.methodNameToAttributeName(method.getName());
                } else {
                    name = annotation.name();
                }
                sortedElements.put(name, () -> {
                    Element attributeElement = xmldoc.createElement("xs:attribute");
                    attributeElement.setAttribute("name", name);
                    attributeElement.setAttribute("type", "xs:string");
                    complexType.appendChild(attributeElement);

                    String desc = annotation.description();
                    if (!desc.isEmpty()) {
                        Element annotationElement = xmldoc.createElement("xs:annotation");
                        attributeElement.appendChild(annotationElement);

                        Element documentationElement = xmldoc.createElement("xs:documentation");
                        documentationElement.setTextContent(annotation.description());
                        annotationElement.appendChild(documentationElement);
                    }
                });
            }
        }

        // write out ordered and duplicates weeded out elements
        for (Map.Entry<String, DelayingElementWriter> entry : sortedElements.entrySet()) {
            entry.getValue().writeElement();
        }

        complexType.appendChild(xmldoc.createElement("xs:anyAttribute"));

        return classElement;
    }

    private interface DelayingElementWriter {
        void writeElement();
    }
}
