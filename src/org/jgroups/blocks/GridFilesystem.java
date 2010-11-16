package org.jgroups.blocks;

import org.jgroups.annotations.Experimental;

import java.io.*;

/**
 * Entry point for GridFile and GridInputStream / GridOutputStream
 * @author Bela Ban
 */
@Experimental
public class GridFilesystem {
    protected final ReplCache<String,byte[]>             data;
    protected final ReplCache<String,GridFile.Metadata>  metadata;
    protected final int                                  default_chunk_size;
    protected final short                                default_repl_count;



    /**
     * Creates an instance. The data and metadata caches should already have been setup and started
     * @param data
     * @param metadata
     * @param default_chunk_size
     */
    public GridFilesystem(ReplCache<String, byte[]> data, ReplCache<String, GridFile.Metadata> metadata,
                          short default_repl_count, int default_chunk_size) {
        this.data=data;
        this.metadata=metadata;
        this.default_chunk_size=default_chunk_size;
        this.default_repl_count=default_repl_count;
    }

    public GridFilesystem(ReplCache<String, byte[]> data, ReplCache<String, GridFile.Metadata> metadata) {
        this(data, metadata, (short)1, 8000);
    }

    public File getFile(String pathname) {
        return getFile(pathname, default_chunk_size);
    }

    public File getFile(String pathname, int chunk_size) {
        return new GridFile(pathname, metadata, chunk_size, this);
    }

    public File getFile(String parent, String child) {
        return getFile(parent, child, default_chunk_size);
    }

    public File getFile(String parent, String child, int chunk_size) {
        return new GridFile(parent, child, metadata, chunk_size, this);
    }

    public File getFile(File parent, String child) {
        return getFile(parent, child, default_chunk_size);
    }

    public File getFile(File parent, String child, int chunk_size) {
        return new GridFile(parent, child, metadata, chunk_size, this);
    }

    public OutputStream getOutput(String pathname) throws IOException {
        return getOutput(pathname, false, default_repl_count, default_chunk_size);
    }

    public OutputStream getOutput(String pathname, boolean append) throws IOException {
        return getOutput(pathname, append, default_repl_count, default_chunk_size);
    }

    public OutputStream getOutput(String pathname, boolean append, short repl_count, int chunk_size) throws IOException {
        GridFile file=(GridFile)getFile(pathname, chunk_size);
        if(!file.createNewFile())
            throw new IOException("creation of " + pathname + " failed");

        return new GridOutputStream(file, append, data, repl_count, chunk_size);
    }

    public OutputStream getOutput(GridFile file) throws IOException {
        if(!file.createNewFile())
            throw new IOException("creation of " + file + " failed");
        return new GridOutputStream(file, false, data, default_repl_count, default_chunk_size);
    }
    


    public InputStream getInput(String pathname) throws FileNotFoundException {
        GridFile file=(GridFile)getFile(pathname);
        if(!file.exists())
            throw new FileNotFoundException(pathname);
        return new GridInputStream(file, data, default_chunk_size);
    }

    public InputStream getInput(File pathname) throws FileNotFoundException {
        return pathname != null? getInput(pathname.getPath()) : null;
    }


    public void remove(String path, boolean synchronous) {
        if(path == null)
            return;
        GridFile.Metadata md=metadata.get(path);
        if(md == null)
            return;
        int num_chunks=md.getLength() / md.getChunkSize() + 1;
        for(int i=0; i < num_chunks; i++)
            data.remove(path + ".#" + i, synchronous);
    }
}
