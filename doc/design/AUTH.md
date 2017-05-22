# Design of authentication protocol (AUTH)

Author: Roland Raez, Bela Ban, Chris Mills

Goal: to prevent unauthorized members from joining a group. Members have to pass authentication
to join a group, otherwise they will be rejected.

## Definition

_[pasted from JGroupsAUTH wiki]_

`AUTH` is used to provide a layer of authentication to JGroups. This allows you to define pluggable security
that defines if a node should be allowed to join a group. AUTH sits below the GMS protocol and listens for
`JOIN REQUEST` messages. When a `JOIN REQUEST` is received it tries to find an `AuthHeader` object, inside of which
should be an implementation of the `AuthToken` object.

`AuthToken` is an abstract class, implementations of which are responsible for providing the actual authentication
mechanism. Some basic implementations of AuthToken are provided in the
`org.jgroups.auth` package (`SimpleToken`, `MD5Token`, `X509Token`, etc).
Effectively all these implementations do is encrypt a string (found in the jgroups config) and pass that on
the `JOIN REQUEST`.

When authentication is successful, the message is simply passed up the stack to the `GMS` protocol.
When it fails, the `AUTH` protocol creates a `JOIN RESPONSE` message with a failure string and passes
it back down the stack. This failure string informs the client of the reason for failure. Clients will
then fail to join the group and will throw a `SecurityException`. If this error string is `null` then
authentication is considered to have passed.

## Example Configuration

```xml
<AUTH auth_class="org.jgroups.auth.X509Token1"
      auth_value="chris_mills_110"
      keystore_path="C\:\Documents and Settings\spare1\.keystore"
      keystore_password="changeit"
      cert_alias="test"
      cipher_type="RSA"/>
```

In the above example the `AUTH` protocol delegates authentication to an instance of the
`org.jgroups.auth.X509Token` class. The only parameter that `AUTH` requires is the `auth_class`
attribute which defines the authentication mechanism. All other parameters defined in the configuration are
passed in to the instance of the `auth_class`.

This allows pluggable authentication mechanisms, abstracted from the core of JGroups, to be configured to
secure and lock down who can join a group.

Creating an AUTH module

   1. Create a class that extends `org.jgroups.auth.AuthToken`
   2. It must have an empty constructor
   3. Implement the `public void setValue(Properties properties)` method to receive properties from the JGroups config.
   4. Implement the `public String getName()` method to return the package and class name
   5. Implement the `public boolean authenticate(AuthToken token)` method to provide the actual authentication
      mechanism of clients.
   6. In the jgroups config XML for `AUTH` set the `auth_class` attribute to your new authentication class. Remember
      to include any other properties your class may require.

## Example Failure

When authentication fails a SecurityException? is thrown on the client trying to join the group. Below is
an example stack trace:

```
org.jboss.jgroups.fileshare.exception.FileShareException: org.jgroups.ChannelException: connect() failed
	at org.jboss.jgroups.fileshare.FileShare.<init>(FileShare.java:28)
	at org.jboss.jgroups.fileshare.FileShare.main(FileShare.java:55)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
	at java.lang.reflect.Method.invoke(Method.java:585)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:78)
Caused by: org.jgroups.ChannelException: connect() failed
	at org.jgroups.JChannel.connect(JChannel.java:425)
	at org.jboss.jgroups.fileshare.FileShare.<init>(FileShare.java:21)
	... 6 more
Caused by: java.lang.SecurityException: Authentication failed
	at org.jgroups.protocols.pbcast.ClientGmsImpl.join(ClientGmsImpl.java:132)
	at org.jgroups.protocols.pbcast.GMS.down(GMS.java:738)
	at org.jgroups.stack.DownHandler.run(Protocol.java:120)
```

On the coordinator the following is displayed for every failed membership join event:

```
21125 [WARN] X509Token.authenticate(): - X509 authentication failed
21125 [WARN] AUTH.up(): - AUTH failed to validate AuthHeader token
```

_[pasted from Roland's email]_

Recently I discussed with Bela Ban about a new Protocol called AUTH to
authenticate the joining of members.

I would like to implement this protocol. Here are my thoughts.

AUTH is the next layer under pbcast.GMS.  When pbcast.GMS sends a join
request (a GMS.GmsHeader.JOIN_REQ  header) AUTH just places the credential
for the authentication in its own header. The join message will be sent to
the coordinator and before the join will reach the GMS protocol the AUTH
protocol checks the auth header. When the header authenticated successfully,
it just passes up the join request and pbcast.GMS will accept the join and
sends the header GMS.GmsHeader.JOIN_RSP containing a pbcast.JoinRsp back.
When the authentication is not successful the AUTH protocol answers the
GMS.GmsHeader.JOIN_REQ itself and sends back a GMS.GmsHeader.JOIN_RSP
containing a pbcast.JoinRsp object with an error message.

The pbcast.ClientGmsImpl receives in both cases the GMS.GmsHeader.JOIN_RSP.
In the case where the join request could not be successfully authenticated
the message in pbcast.JoinRsp can be used to throw a RuntimeException.

Requirements for other Protocols:
 - pbcast.JoinRsp needs a new attribute (String), e.g. errorMsg
 - pbcast.ClientGmsImpl should use the attribute defined above and throw an
exception when there was an error.
 - Optionally ENCRYPT can be extended so that AUTH provides the symmetric
key

I think it would be fine when AUTH supports two authentication methods:

Token:
A simple token (String) based authentication using a configured String or a
String read from an external file which can be protected so that only
certain processes can read the credential. Each member in the group needs to
have the same token else the authentication will not succeed:

 1. AUTH sends along with the GMS.GmsHeader.JOIN_REQ the hash of the
credential including its address (each address will produce another hash).
 2. The coordinator compares it's own hash with the one from the client.
When they match the JOIN_REQ is passed up, else negative JOIN_RSP (with
errorMsg set) is sent back (--> no further steps).
 3. Along with the positive response JOIN_RSP from the GMS protocol the
coordinator sends the hash of the credential including its address.
 4. The client verifies the hash of the coordinator. When the verification
is ok, he passes up the JOIN_RSP else he sends down a LEAVE_REQ.
Should the following LEAVE_RSP be discarded by the AUTH protocol layer?

Certificate:
Implementing Certificate based authentication is more secure because
spoofing IP addresses doesn't impact the security when additional message
encryption is used. I think that it is a requirement that each member in the
group can have the same certificate.
The following algorithm allows the usage of the same certificate for each
member in the group.



 1. The joiner (GMS client) sends with the JOIN_REQ message its certificate
and a nonce.
 2. The coordinator verifies if he trusts the certificate of the client by
checking the certificate chain. The authentication of the joiner is
performed in a later step! When he does not trust, he sends back a negative
GMS.GmsHeader.JOIN_RSP (--> no further steps).
 3. The coordinator create a AUTH.REQ message containing its certificate,
the nonce from the joiner encrypted with the private key of its certificate
and a new nonce
 4. The joiner verifies if he trusts the server by verifying the message
(decrypts the nonce with the public key from the coordinator (retrieved from
the certificate)) and optionally by checking the certificate chain. When the
verification is ok, he creates an AUTH.RSP message containing the nonce from
the coordinator encrypted with the private key of the joiner. When the
verification is not successful AUTH.RSP message containing an error message.
 5. The coordinator verifies the AUTH.RSP message. When it contains an error
message a negative JOIN_RSP is sent down else the joiner is authenticated by
decrypting the nonce with the public key of the joiner. When this is ok a
JOIN_REQ is sent up to GMS else a negative JOIN_RSP is sent down.
 6. The GMS protocols answers with a JOIN_RSP. When ENCRYPT and AUTH work
together the AUTH protocol or the ENCRYPT protocol sends along this JOIN_RSP
the new symmetric encryption key encrypted with the public key of the
joiner.

This protocol uses a lot of public private key encryptions (4 or five when
AUTH provides the key for ENCRYPT) but I think this is ok (members usually
join not very often).


The session key created by the coordinator could be used in the ENCRYPT
protocol for the message encryption.


For me there are the following open questions:
 - Are the two AUTH methods reasonable?
 - Should ENCRYPT be extended so that the AUTH protocol distributes the new
symmetric encryption key?
 - How would AUTH "send" the key to the ENCRYPT protocol? Using a header
which is cleared in the ENCRYPT layer would need that the ENCRYPT must be
below AUTH because of fragmentation. This would be ok for me.
 - Would it be ok that when a member leaves the group that still the same
symmetric key is used?