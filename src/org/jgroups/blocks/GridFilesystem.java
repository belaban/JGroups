package org.jgroups.blocks;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * Entry point for GridFile and GridInputStream / GridOutputStream
 * @author Bela Ban
 * @version $Id: GridFilesystem.java,v 1.3 2009/12/23 14:13:42 belaban Exp $
 */
public class GridFilesystem {
    protected final ReplCache<String,byte[]>             data;
    protected final ReplCache<String,GridFile.Metadata>  metadata;
    protected final int default_chunk_size;


    public GridFilesystem(ReplCache<String, byte[]> data, ReplCache<String, GridFile.Metadata> metadata) {
        this(data, metadata, 4000);
    }

    /**
     * Creates an instance. The data and metadata caches should already have been setup and started
     * @param data
     * @param metadata
     * @param default_chunk_size
     */
    public GridFilesystem(ReplCache<String, byte[]> data, ReplCache<String, GridFile.Metadata> metadata, int default_chunk_size) {
        this.data=data;
        this.metadata=metadata;
        this.default_chunk_size=default_chunk_size;
    }

    public File getFile(String pathname) {
        return getFile(pathname, default_chunk_size);
    }

    public File getFile(String pathname, int chunk_size) {
        return new GridFile(pathname, metadata, chunk_size);
    }

    public File getFile(String parent, String child) {
        return getFile(parent, child, default_chunk_size);
    }

    public File getFile(String parent, String child, int chunk_size) {
        return new GridFile(parent, child, metadata, chunk_size);
    }

    public File getFile(File parent, String child) {
        return getFile(parent, child, default_chunk_size);
    }

    public File getFile(File parent, String child, int chunk_size) {
        return new GridFile(parent, child, metadata, chunk_size);
    }

    public OutputStream getOutput(String pathname) throws IOException {
        return getOutput(pathname, false);
    }

    public OutputStream getOutput(String pathname, boolean append) throws IOException {
        return getOutput(pathname, append, default_chunk_size);
    }

    public OutputStream getOutput(String pathname, boolean append, int chunk_size) throws IOException {
        GridFile file=(GridFile)getFile(pathname, chunk_size);
        if(!file.createNewFile())
            throw new IOException("creation of " + pathname + " failed");

        // return new GridOutputStream(pathname, file, data, chunk_size);

        throw new UnsupportedOperationException();
    }

    public OutputStream getOutput(File file) {
        return getOutput(file, default_chunk_size);
    }

    public OutputStream getOutput(File file, int chunk_size) {
        throw new UnsupportedOperationException();
    }

    public InputStream getInput(String pathname) {
        throw new UnsupportedOperationException();
    }

    public InputStream getInput(File pathname) {
        throw new UnsupportedOperationException();
    }
}
