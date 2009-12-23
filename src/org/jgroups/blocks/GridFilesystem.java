package org.jgroups.blocks;

import java.io.File;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Entry point for GridFile and GridInputStream / GridOutputStream
 * @author Bela Ban
 * @version $Id: GridFilesystem.java,v 1.1 2009/12/23 13:38:47 belaban Exp $
 */
public class GridFilesystem {
    protected final ReplCache<String,byte[]>             data;
    protected final ReplCache<String,GridFile.Metadata>  metadata;

    /**
     * Creates an instance. The data and metadata caches should already have been setup and started
     * @param data
     * @param metadata
     */
    public GridFilesystem(ReplCache<String, byte[]> data, ReplCache<String, GridFile.Metadata> metadata) {
        this.data=data;
        this.metadata=metadata;
    }

    public File getFile(String pathname) {
        throw new UnsupportedOperationException();
    }

    public File getFile(String parent, String child) {
        throw new UnsupportedOperationException();
    }

    public File getFile(File parent, String child) {
        throw new UnsupportedOperationException();
    }

    public File getFile(URI uri) {
        throw new UnsupportedOperationException();
    }

    public OutputStream getOutput(String pathname) {
        return getOutput(pathname, false);
    }

    public OutputStream getOutput(String pathname, boolean append) {
        throw new UnsupportedOperationException();
    }

    public OutputStream getOutput(File file) {
        throw new UnsupportedOperationException();
    }

    public InputStream getInput(String pathname) {
        throw new UnsupportedOperationException();
    }

    public InputStream getInput(File pathname) {
        throw new UnsupportedOperationException();
    }
}
