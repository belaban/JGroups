package org.jgroups.protocols.jzookeeper;

import oshi.util.FormatUtil;


public class StatsCollector implements Runnable{

	private SystemStats stats;
	private int refreshRateSeconds;
	
	public StatsCollector(SystemStats ss, int seconds){
		this.stats= ss;
		this.refreshRateSeconds = seconds;
	}
	
	@Override
	public void run() {
		long startTime=System.currentTimeMillis();
		while(true){
			if((System.currentTimeMillis()-startTime) > ((long)refreshRateSeconds*1000) ){
				this.stats.getCurrentValues();
				System.out.println("Available Memory over Total: "
						+ FormatUtil.formatBytes(this.stats.getHal().getMemory().getAvailable()) + "/"
						+ FormatUtil.formatBytes(this.stats.getHal().getMemory().getTotal()));
				startTime = System.currentTimeMillis();
			}
		}
	}

}
