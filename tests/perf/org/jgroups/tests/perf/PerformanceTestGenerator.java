package org.jgroups.tests.perf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;


/**
 * Utility tool that creates configuration files for automated performance tests. 
 * PerformanceTestGenerator, given an input file with -config flag, generates 
 * test files that are used in conjunction with bin/clusterperfromancetest.sh. 
 * The number of test files generated is numOfSenders*messageSizes.
 * 
 * Default configuration file used for this utility is conf/performancetestconfig.txt 
 * See bin/clusterperformancetest.sh
 *  
 * @author Vladimir Blagojevic
 * @version $Id$
 */
public class PerformanceTestGenerator {
	Properties configProperties = null;
	private long totalDataBytes;
	private int[] numOfSenders;
	private int[] messageSizes;
	private int numNodes;
	private int interval;
	
	
	public PerformanceTestGenerator(Properties config) {
		configProperties = config;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties config = null;
        for(int i=0; i < args.length; i++) {
            if("-config".equals(args[i])) {
                String config_file=args[++i];
                config = new Properties();
                try {
					config.load(new FileInputStream(config_file));
				} catch (FileNotFoundException e) {
					System.err.println("File " + config_file + " not found");
					return;
				} catch (IOException e) {
					System.err.println("Error reading configuration file " + config_file);
					return;
				}
                continue;
            }
            System.out.println("PerformanceTestGenerator -config <file>");
            return;
        }

        try {
        	PerformanceTestGenerator t=new PerformanceTestGenerator(config);
        	t.parse();
        	t.generateTestConfigFiles();
           
        }
        catch(Exception e) {
            e.printStackTrace();
        }
	}

	private void parse() throws Exception{
		
		numNodes = Integer.parseInt(configProperties.getProperty("nodes"));		
		totalDataBytes = Long.parseLong(configProperties.getProperty("total_data"));
		numOfSenders = tokenizeAndConvert(configProperties.getProperty("number_of_senders"),",");		
		messageSizes = tokenizeAndConvert(configProperties.getProperty("message_sizes"),",");		
		interval = Integer.parseInt(configProperties.getProperty("interval"));
	}
	private void generateFile(int numOfSenders, int messageSize,int nodeCount) {	
		FileWriter fw = null;
		long numOfMessages = (totalDataBytes/messageSize);
		try {
			fw = new FileWriter("config_"+numOfSenders + "_" +nodeCount +"_"+ messageSize + ".txt");
			fw.write("transport=org.jgroups.tests.perf.transports.JGroupsTransport\n");
			fw.write("msg_size="+messageSize+"\n");
			fw.write("num_msgs="+(numOfMessages/numOfSenders) +"\n");
			fw.write("num_senders="+numOfSenders+"\n");
			fw.write("num_members="+nodeCount+"\n");
			fw.write("log_interval="+interval+"\n");
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public static String[] tokenize(String s, String delim) {
		if (s == null || s.length() == 0) {
			return new String[0];
		}
		if (delim == null || delim.length() == 0) {
			return new String[] { s };
		}

		ArrayList tokens = new ArrayList();

		int pos = 0;
		int delPos = 0;
		while ((delPos = s.indexOf(delim, pos)) != -1) {
			tokens.add(s.substring(pos, delPos));
			pos = delim.length() + delPos;
		}
		tokens.add(s.substring(pos));
		return (String[]) tokens.toArray(new String[tokens.size()]);
	}
	
	public static int[] tokenizeAndConvert(String s, String delim) {
		String stokens [] = tokenize(s,delim);
		int result [] = new int[stokens.length];
		for (int i = 0; i < stokens.length; i++) {
			result[i]=Integer.parseInt(stokens[i]);
		}
		return result;
	}

	private void generateTestConfigFiles() {
		for (int i = 0; i < numOfSenders.length; i++) {
			for (int j = 0; j < messageSizes.length; j++) {
				generateFile(numOfSenders[i],messageSizes[j],numNodes);
			}			
		}
	}

}
