package org.jgroups.protocols.dns;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class MockDirContext implements DirContext {

   private static class DNSKey {
      final String query;
      final DNSResolver.DNSRecordType recordType;

      private DNSKey(String query, DNSResolver.DNSRecordType recordType) {
         this.query = query;
         this.recordType = recordType;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         DNSKey dnsKey = (DNSKey) o;

         if (query != null ? !query.equals(dnsKey.query) : dnsKey.query != null) return false;
         return recordType == dnsKey.recordType;
      }

      @Override
      public int hashCode() {
         int result = query != null ? query.hashCode() : 0;
         result = 31 * result + (recordType != null ? recordType.hashCode() : 0);
         return result;
      }
   }

   private Map<DNSKey, Attributes> responseMap = new HashMap<>();

   private MockDirContext() {

   }

   public static MockDirContext newDefault() {
      return new MockDirContext();
   }

   public MockDirContext addEntry(String dnsQuery, String dnsResponse, DNSResolver.DNSRecordType recordType) {
      Attributes attributes = responseMap.computeIfAbsent(new DNSKey(dnsQuery, recordType), v -> new BasicAttributes());
      Attribute attribute = attributes.get(recordType.toString());
      if(attribute == null) {
         attributes.put(new BasicAttribute(recordType.toString(), dnsResponse));
      } else {
         attribute.add(dnsResponse);
      }
      return this;
   }

   @Override
   public Attributes getAttributes(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Attributes getAttributes(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Attributes getAttributes(Name name, String[] attrIds) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Attributes getAttributes(String name, String[] attrIds) throws NamingException {
      DNSResolver.DNSRecordType recordType = DNSResolver.DNSRecordType.valueOf(attrIds[0]);
      return responseMap.get(new DNSKey(name, recordType));
   }

   @Override
   public void modifyAttributes(Name name, int mod_op, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void modifyAttributes(String name, int mod_op, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void modifyAttributes(Name name, ModificationItem[] mods) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void modifyAttributes(String name, ModificationItem[] mods) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void bind(Name name, Object obj, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void bind(String name, Object obj, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void rebind(Name name, Object obj, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void rebind(String name, Object obj, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public DirContext createSubcontext(Name name, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public DirContext createSubcontext(String name, Attributes attrs) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public DirContext getSchema(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public DirContext getSchema(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public DirContext getSchemaClassDefinition(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public DirContext getSchemaClassDefinition(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(Name name, Attributes matchingAttributes, String[] attributesToReturn) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(String name, Attributes matchingAttributes, String[] attributesToReturn) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(Name name, Attributes matchingAttributes) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(String name, Attributes matchingAttributes) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(Name name, String filter, SearchControls cons) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(String name, String filter, SearchControls cons) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(Name name, String filterExpr, Object[] filterArgs, SearchControls cons) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<SearchResult> search(String name, String filterExpr, Object[] filterArgs, SearchControls cons) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Object lookup(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Object lookup(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void bind(Name name, Object obj) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void bind(String name, Object obj) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void rebind(Name name, Object obj) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void rebind(String name, Object obj) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void unbind(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void unbind(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void rename(Name oldName, Name newName) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void rename(String oldName, String newName) throws NamingException {
      // TODO: Customise this generated block
   }

   @Override
   public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void destroySubcontext(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void destroySubcontext(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Context createSubcontext(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Context createSubcontext(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Object lookupLink(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Object lookupLink(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NameParser getNameParser(Name name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public NameParser getNameParser(String name) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Name composeName(Name name, Name prefix) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public String composeName(String name, String prefix) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Object addToEnvironment(String propName, Object propVal) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Object removeFromEnvironment(String propName) throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Hashtable<?, ?> getEnvironment() throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void close() throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public String getNameInNamespace() throws NamingException {
      throw new UnsupportedOperationException("Not implemented");
   }
}
