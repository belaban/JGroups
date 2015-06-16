AZURE_PING
==========

Author: Radoslav Husar
Version: June 2015

Goal
----

Goal is to provide efficient discovery implementation for Microsoft Windows Azure environment besides options such
as JDBC_PING. It is based on blob storage.


Usage
-----

To use AZURE_PING add the implementing protocol, org.jgroups.protocols.AZURE_PING, to the stack and configure the
following properties needed to authenticate and use the Azure Storage service:

 * storage_account_name - the name of the storage account
 * storage_access_key - the secret access key
 * container - the name of the container to use for PING data

To find out these values, do the following:

 * Navigate to the Azure management portal at [https://manage.windowsazure.com/](https://manage.windowsazure.com/)
 * Create new storage service via NEW &gt; DATA SERVICES &gt; STORAGE &gt; QUICK CREATE and provide desired
   configuration
 * Navigate to STORAGE &gt; (storage account name) &gt; MANAGE ACCESS KEYS

 _Described as of Azure console version June 2015._


Design
------

Just like other cloud-based PING protocols, the implementation is based on the original FILE_PING. All ping information
is stored in the the configured storage container. If a container with that name does not exist, it will be
automatically created.

Within the container, ping data is stored in flat files with names constructed as:
FILENAME = CLUSTER_NAME + "-" + ADDRESS

The Azure client is embedded in the protocol implementation. The client itself uses a REST client underneath.


Resources
---------

* Azure console - [https://manage.windowsazure.com/](https://manage.windowsazure.com/)
* Azure documentation - [https://azure.microsoft.com/en-us/documentation/](https://azure.microsoft.com/en-us/documentation/)
* Azure Java SDK on GitHub - [https://github.com/Azure/azure-sdk-for-java](https://github.com/Azure/azure-sdk-for-java)
* Azure JavaDoc - [http://dl.windowsazure.com/javadoc/](http://dl.windowsazure.com/javadoc/)

