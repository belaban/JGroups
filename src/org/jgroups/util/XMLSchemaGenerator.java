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

    protected static final String[] PACKAGES={"", "pbcast", "relay", "rules", "dns", "kubernetes"};

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
        try(FileWriter fw = new FileWriter(f, false)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            DOMImplementation impl = builder.getDOMImplementation();
            Document xmldoc = impl.createDocument("http://www.w3.org/2001/XMLSchema", "xs:schema", null);
            Element doc_el=xmldoc.getDocumentElement();
            doc_el.setAttribute("targetNamespace", "urn:org:jgroups");
            doc_el.setAttribute("xmlns:tns", "urn:org:jgroups");
            doc_el.setAttribute("elementFormDefault", "qualified");
            doc_el.setAttribute("attributeFormDefault", "unqualified");

            Element complexType = xmldoc.createElement("xs:complexType");
            complexType.setAttribute("name", "ConfigType");
            doc_el.appendChild(complexType);

            Element allType = xmldoc.createElement("xs:choice");
            allType.setAttribute("maxOccurs", "unbounded");
            complexType.appendChild(allType);
            generateProtocolSchema(xmldoc, allType, PACKAGES);

            Element xsElement = xmldoc.createElement("xs:element");
            xsElement.setAttribute("name", "config");
            xsElement.setAttribute("type", "tns:ConfigType");
            doc_el.appendChild(xsElement);

            DOMSource domSource = new DOMSource(xmldoc);
            StreamResult streamResult = new StreamResult(fw);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer serializer = tf.newTransformer();
            serializer.setOutputProperty(OutputKeys.METHOD, "xml");
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            serializer.transform(domSource, streamResult);

            fw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<Class<?>> getClassesFromPackages(Class<?> cl, String ... packages) throws ClassNotFoundException {
        List<Class<?>> sortedClasses = new LinkedList<>();
        for(String suffix: packages) {
            Package p=Package.getPackage(suffix);
            String package_name=p != null? suffix : PROT_PACKAGE + (suffix == null || suffix.isEmpty()? "" : "." + suffix);
            Set<Class<?>> classes=getClasses(cl, package_name);
            sortedClasses.addAll(classes);
        }
        sortedClasses.sort(Comparator.comparing(Class::getCanonicalName));
        return sortedClasses;
    }

    protected static void generateProtocolSchema(Document xmldoc, Element parent, String... suffixes) throws Exception {
        for(String suffix: suffixes) {
            String package_name=PROT_PACKAGE + (suffix == null || suffix.isEmpty()? "" : "." + suffix);
            Set<Class<?>> classes=getClasses(Protocol.class, package_name);
            List<Class<?>> sortedClasses = new LinkedList<>(classes);
            sortedClasses.sort(Comparator.comparing(Class::getCanonicalName));
            for (Class<?> clazz : sortedClasses)
                classToXML(xmldoc, parent, clazz, package_name);
        }
    }


    protected static List<Class<?>> findImplementations(List<Class<?>> classes, Class<?> intf) {
        ArrayList<Class<?>> retval=new ArrayList<>();
        for(Class<?> cl: classes) {
            if(cl.isAssignableFrom(intf)) {
                retval.add(cl);
            }
            if(intf.isAssignableFrom(cl))
                retval.add(cl);
        }

        return retval;
    }


    public static Set<Class<?>> getClasses(Class<?> assignableFrom, String packageName) throws ClassNotFoundException {
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
        if(elementName.isEmpty())
            throw new IllegalArgumentException("Cannot create empty attribute name for element xs:element, class is " + clazz);

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

        Map<String, DelayingElementWriter> sortedElements=new TreeMap<>();

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


        iterateOverAttributes(clazz, sortedElements, xmldoc, complexType, null);

        // iterate over the components (fields)
        Util.forAllComponentTypes(clazz, (cl, prefix) -> {
            iterateOverAttributes(cl, sortedElements, xmldoc, complexType, prefix);
            if(cl.isInterface()) { // for example Bundler, let's add attributes from implementations
                try {
                    List<Class<?>> classes=getClassesFromPackages(cl, cl.getPackageName());
                    for(Class<?> c: classes)
                        iterateOverAttributes(c, sortedElements, xmldoc, complexType, prefix);
                }
                catch(ClassNotFoundException e) {
                }
            }
        });

        iterateOverMethods(clazz, sortedElements, xmldoc, complexType, null);

        // iterate over the components (methods)
        Util.forAllComponentTypes(clazz, (cl, prefix) -> {
            iterateOverMethods(cl, sortedElements, xmldoc, complexType, prefix);
            if(cl.isInterface()) {
                try {
                    List<Class<?>> classes=getClassesFromPackages(cl, cl.getPackageName());
                    for(Class<?> c: classes)
                        iterateOverMethods(c, sortedElements, xmldoc, complexType, prefix);
                }
                catch(ClassNotFoundException e) {
                }
            }
        });

        // write out ordered and duplicates weeded out elements
        for (Map.Entry<String, DelayingElementWriter> entry : sortedElements.entrySet()) {
            entry.getValue().writeElement();
        }

        complexType.appendChild(xmldoc.createElement("xs:anyAttribute"));
        return classElement;
    }

    protected static void iterateOverAttributes(Class<?> clazz, Map<String,DelayingElementWriter> sortedElements,
                                                Document xmldoc, Element complexType, String prefix) {
        Field[] fields=Util.getAllDeclaredFieldsWithAnnotations(clazz, Property.class);
        for (Field field: fields) {
            final Property r = field.getAnnotation(Property.class);
            boolean annotationRedefinesName = !r.name().isEmpty() && r.deprecatedMessage().isEmpty();
            final String attr_name=annotationRedefinesName? r.name() : field.getName();
            if(attr_name == null || attr_name.isEmpty())
                throw new IllegalArgumentException("Cannot create empty attribute name for element xs:attribute, field is " + field);

            String tmp_attrname=prefix != null && !prefix.trim().isEmpty()? prefix + "." + attr_name : attr_name;
            sortedElements.put(tmp_attrname, () -> {
                Element attributeElement = xmldoc.createElement("xs:attribute");
                attributeElement.setAttribute("name", tmp_attrname);

                // Agreement with Bela Ban on Jan-20-2009 (Go Obama!!!) to treat all types as xs:string since we do not
                // know where users are going to use replacement tokens in configuration files. Therefore, the type
                // becomes indeterminate
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


    protected static void iterateOverMethods(Class<?> clazz, Map<String,DelayingElementWriter> sortedElements,
                                             Document xmldoc, Element complexType, String prefix) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Property.class)) {
                final Property ann = method.getAnnotation(Property.class);
                String name=ann.name().length() < 1? Util.methodNameToAttributeName(method.getName()) : ann.name();
                String tmp_name=prefix != null && !prefix.trim().isEmpty()? prefix + "." + name : name;
                sortedElements.put(tmp_name, () -> {
                    Element attributeElement = xmldoc.createElement("xs:attribute");

                    attributeElement.setAttribute("name", tmp_name);
                    attributeElement.setAttribute("type", "xs:string");
                    complexType.appendChild(attributeElement);

                    String desc = ann.description();
                    if (!desc.isEmpty()) {
                        Element annotationElement = xmldoc.createElement("xs:annotation");
                        attributeElement.appendChild(annotationElement);

                        Element documentationElement = xmldoc.createElement("xs:documentation");
                        documentationElement.setTextContent(ann.description());
                        annotationElement.appendChild(documentationElement);
                    }
                });
            }
        }
    }

    private interface DelayingElementWriter {
        void writeElement();
    }
}
