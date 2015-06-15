package org.jgroups.protocols;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Responses;

/**
 * Implementation of PING protocol for AZURE using Storage Blobs. See doc/design/AZURE_PING.md for design.
 *
 * @author Radoslav Husar
 * @version Jun 2015
 */
public class AZURE_PING extends FILE_PING {

    private static final Log log = LogFactory.getLog(AZURE_PING.class);

    @Property(description = "The name of the storage account.")
    protected String storage_account_name;

    @Property(description = "The secret account access key.", exposeAsManagedAttribute = false)
    protected String storage_access_key;

    @Property(description = "Container to store ping information in. Must be valid DNS name.")
    protected String container;

    @Property(description = "Whether or not to use HTTPS to connect to Azure.")
    protected boolean use_https = true;

    public static final int STREAM_BUFFER_SIZE = 4096;

    private CloudBlobContainer containerReference;

    @Override
    public void init() throws Exception {
        super.init();

        // Validate configuration
        // Can throw IAEs
        this.validateConfiguration();

        try {
            StorageCredentials credentials = new StorageCredentialsAccountAndKey(storage_account_name, storage_access_key);
            CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, use_https);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            containerReference = blobClient.getContainerReference(container);
            boolean created = containerReference.createIfNotExists();

            if (created) {
                log.info("Created container named '%s'.", container);
            } else {
                log.debug("Using existing container named '%s'.", container);
            }

        } catch (Exception ex) {
            log.error("Error creating a storage client! Check your configuration.", ex);
        }
    }

    public void validateConfiguration() throws IllegalArgumentException {
        // Validate that container name is configured and must be all lowercase
        if (container == null || !container.toLowerCase().equals(container) || container.contains("--")
                || container.startsWith("-") || container.length() < 3 || container.length() > 63) {
            throw new IllegalArgumentException("Container name must be configured and must meet Azure requirements (must be a valid DNS name).");
        }
        // Account name and access key must be both configured for write access
        if (storage_account_name == null || storage_access_key == null) {
            throw new IllegalArgumentException("Account name and key must be configured.");
        }
        // Lets inform users here that https would be preferred
        if (!use_https) {
            log.info("Configuration is using HTTP, consider switching to HTTPS instead.");
        }

    }

    @Override
    protected void createRootDir() {
        // Do not remove this!
        // There is no root directory to create, overriding here with noop.
    }

    @Override
    protected void readAll(final List<Address> members, final String clustername, final Responses responses) {
        if (clustername == null) {
            return;
        }

        String prefix = sanitize(clustername);

        Iterable<ListBlobItem> listBlobItems = containerReference.listBlobs(prefix);
        for (ListBlobItem blobItem : listBlobItems) {
            try {
                // If the item is a blob and not a virtual directory.
                // n.b. what an ugly API this is
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    ByteArrayOutputStream os = new ByteArrayOutputStream(STREAM_BUFFER_SIZE);
                    blob.download(os);
                    byte[] pingBytes = os.toByteArray();
                    parsePingData(pingBytes, members, responses);
                }
            } catch (Exception t) {
                log.error("Error fetching ping data.");
            }
        }
    }

    protected void parsePingData(final byte[] pingBytes, final List<Address> members, final Responses responses) {
        if (pingBytes == null || pingBytes.length <= 0) {
            return;
        }
        List<PingData> list;
        try {
            list = read(new ByteArrayInputStream(pingBytes));
            if (list != null) {
                // This is a common piece of logic for all PING protocols copied from org/jgroups/protocols/FILE_PING.java:245
                // Maybe could be extracted for all PING impls to share this logic?
                for (PingData data : list) {
                    if (members == null || members.contains(data.getAddress())) {
                        responses.addResponse(data, data.isCoord());
                    }
                    if (local_addr != null && !local_addr.equals(data.getAddress())) {
                        addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
                    }
                }
                // end copied block
            }
        } catch (Exception e) {
            log.error("Error unmarshalling ping data.", e);
        }
    }

    @Override
    protected void write(final List<PingData> list, final String clustername) {
        if (list == null || clustername == null) {
            return;
        }

        String filename = addressToFilename(clustername, local_addr);
        ByteArrayOutputStream out = new ByteArrayOutputStream(STREAM_BUFFER_SIZE);

        try {
            write(list, out);
            byte[] data = out.toByteArray();

            // Upload the file
            CloudBlockBlob blob = containerReference.getBlockBlobReference(filename);
            blob.upload(new ByteArrayInputStream(data), data.length);

        } catch (Exception ex) {
            log.error("Error marshalling and uploading ping data.", ex);
        }

    }

    @Override
    protected void remove(final String clustername, final Address addr) {
        if (clustername == null || addr == null) {
            return;
        }

        String filename = addressToFilename(clustername, addr);

        try {
            CloudBlockBlob blob = containerReference.getBlockBlobReference(filename);
            boolean deleted = blob.deleteIfExists();

            if (deleted) {
                log.debug("Tried to delete file '%s' but it was already deleted.", filename);
            } else {
                log.trace("Deleted file '%s'.", filename);
            }

        } catch (Exception ex) {
            log.error("Error deleting files.", ex);
        }
    }

    @Override
    protected void removeAll(String clustername) {
        if (clustername == null) {
            return;
        }

        clustername = sanitize(clustername);

        Iterable<ListBlobItem> listBlobItems = containerReference.listBlobs(clustername);
        for (ListBlobItem blobItem : listBlobItems) {
            try {
                // If the item is a blob and not a virtual directory.
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    boolean deleted = blob.deleteIfExists();
                    if (deleted) {
                        log.debug("Tried to delete file '%s' but it was already deleted.", blob.getName());
                    } else {
                        log.trace("Deleted file '%s'.", blob.getName());
                    }
                }
            } catch (Exception e) {
                log.error("Error deleting ping data for cluster '" + clustername + "'.", e);
            }
        }
    }

    /**
     * Converts cluster name and address into a filename.
     */
    protected static String addressToFilename(final String clustername, final Address address) {
        return sanitize(clustername) + "-" + addressToFilename(address);
    }

    /**
     * Sanitizes names replacing backslashes and forward slashes with a dash.
     */
    protected static String sanitize(final String name) {
        return name.replace('/', '-').replace('\\', '-');
    }


}
