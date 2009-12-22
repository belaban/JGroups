package org.jgroups.tests;

import org.jgroups.blocks.ReplCache;
import org.jgroups.blocks.GridFile;
import org.jgroups.blocks.Cache;

import java.io.File;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: GridFileTest.java,v 1.2 2009/12/22 16:29:05 belaban Exp $
 */
public class GridFileTest {

    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String cluster_name="imfs-cluster";
        final int chunk_size=4000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-cluster_name")) {
                cluster_name=args[++i];
                continue;
            }
            System.out.println("GridFileTest [-props <JGroups config>] [-cluster_name <cluster name]");
            return;
        }


        ReplCache<String, GridFile.Metadata> cache=new ReplCache<String, GridFile.Metadata>(props, cluster_name);
        cache.start();

        File file=new GridFile("/home/bela/grid", cache, chunk_size);
        file.mkdirs();
        System.out.println("input file = " + file + " \n");

        for(String name: new String[]{"a.txt", "b.txt", "one.log"}) {
            File f2=new GridFile(file, name, cache, chunk_size);
            f2.createNewFile();
        }

        File subdir=new GridFile(file, "docs", cache, chunk_size);
        subdir.mkdir();
        for(String name: new String[]{"data.log", "c.txt", "two.log"}) {
            File f2=new GridFile(subdir, name, cache, chunk_size);
            f2.createNewFile();
        }

        subdir=new GridFile(file, "src", cache, chunk_size);
        subdir.mkdir();
        for(String name: new String[]{"bla.java", "foo.java", "foobar.java"}) {
            File f2=new GridFile(subdir, name, cache, chunk_size);
            f2.createNewFile();
        }

        String[] list=file.list();
        if(list != null) {
            for(String filename: list)
                System.out.println("file = " + filename);
            System.out.println("(" + list.length + " children)");
        }

       /* for(Map.Entry<String,Cache.Value<ReplCache.Value<GridFile.Metadata>>> entry: cache.getL2Cache().entrySet()) {
            String key=entry.getKey();
            Cache.Value<ReplCache.Value<GridFile.Metadata>> val=entry.getValue();
            System.out.println(key + ": " + val.getValue().getVal());
        }*/
        

        cache.stop();
    }
}