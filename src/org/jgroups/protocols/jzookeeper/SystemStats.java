package org.jgroups.protocols.jzookeeper;

import java.util.ArrayList;

//import oshi.SystemInfo;
//import oshi.hardware.HardwareAbstractionLayer;
//import oshi.hardware.Processor;
//import oshi.software.os.OperatingSystem;


public class SystemStats {

	private String osName;
	//private HardwareAbstractionLayer hal;
	
	private int cpuNumber;
	private String cpuVendor;
	private int cpuCores;
	private long cpuFreq;
	
	private ArrayList<Double> coresLoad;
	
	private long memAvailable;
	private long memTotal;
	
	private double systemLoad;
	private double systemLoadAverage;
	
	
	public SystemStats() {
		initAll();
	}
	
	public void initAll(){
//		SystemInfo si = new SystemInfo();
//		OperatingSystem os = si.getOperatingSystem();
		
	//	this.osName = os.toString();
		//this.hal = si.getHardware();
		//this.cpuNumber = hal.getProcessors().length;
		this.coresLoad = new ArrayList<Double>(this.cpuNumber);
		
		//this.memAvailable =  hal.getMemory().getAvailable();
		//this.memTotal = hal.getMemory().getTotal();
		
//		for (Processor cpu : hal.getProcessors()) {
//			this.coresLoad.add( cpu.getSystemLoadAverage() );	
//			
//			this.systemLoad = cpu.getSystemCpuLoad();
//			this.systemLoadAverage  = cpu.getSystemLoadAverage();
//			this.cpuVendor = cpu.getVendor();
//			this.cpuFreq = cpu.getVendorFreq();
//		}
		
	}
	
	
	public void getCurrentValues(){
//		this.memAvailable =  hal.getMemory().getAvailable();
//		this.memTotal = hal.getMemory().getTotal();
		
		//for (Processor cpu : hal.getProcessors()) {
//			this.coresLoad.add( cpu.getSystemLoadAverage() );	
//			
//			this.systemLoad = cpu.getSystemCpuLoad();
//			this.systemLoadAverage  = cpu.getSystemLoadAverage();
//			this.cpuVendor = cpu.getVendor();
//			this.cpuFreq = cpu.getVendorFreq();
//		}
	}
	

	/**
	 * @return the osName
	 */
	public String getOsName() {
		return osName;
	}

	/**
	 * @param osName the osName to set
	 */
	public void setOsName(String osName) {
		this.osName = osName;
	}

	/**
	 * @return the cpuNumber
	 */
	public int getCpuNumber() {
		return cpuNumber;
	}

	/**
	 * @param cpuNumber the cpuNumber to set
	 */
	public void setCpuNumber(int cpuNumber) {
		this.cpuNumber = cpuNumber;
	}

	/**
	 * @return the cpuVendor
	 */
	public String getCpuVendor() {
		return cpuVendor;
	}

	/**
	 * @param cpuVendor the cpuVendor to set
	 */
	public void setCpuVendor(String cpuVendor) {
		this.cpuVendor = cpuVendor;
	}

	/**
	 * @return the cpuCores
	 */
	public int getCpuCores() {
		return cpuCores;
	}

	/**
	 * @param cpuCores the cpuCores to set
	 */
	public void setCpuCores(int cpuCores) {
		this.cpuCores = cpuCores;
	}

	/**
	 * @return the cpuFreq
	 */
	public long getCpuFreq() {
		return cpuFreq;
	}

	/**
	 * @param cpuFreq the cpuFreq to set
	 */
	public void setCpuFreq(long cpuFreq) {
		this.cpuFreq = cpuFreq;
	}


	/**
	 * @return the memAvailable
	 */
	public long getMemAvailable() {
		return memAvailable;
	}

	/**
	 * @param memAvailable the memAvailable to set
	 */
	public void setMemAvailable(long memAvailable) {
		this.memAvailable = memAvailable;
	}

	/**
	 * @return the memTotal
	 */
	public long getMemTotal() {
		return memTotal;
	}

	/**
	 * @param memTotal the memTotal to set
	 */
	public void setMemTotal(long memTotal) {
		this.memTotal = memTotal;
	}

	/**
	 * @return the systemLoad
	 */
	public double getSystemLoad() {
		return systemLoad;
	}

	/**
	 * @param systemLoad the systemLoad to set
	 */
	public void setSystemLoad(double systemLoad) {
		this.systemLoad = systemLoad;
	}

	/**
	 * @return the systemLoadAverage
	 */
	public double getSystemLoadAverage() {
		return systemLoadAverage;
	}

	/**
	 * @param systemLoadAverage the systemLoadAverage to set
	 */
	public void setSystemLoadAverage(double systemLoadAverage) {
		this.systemLoadAverage = systemLoadAverage;
	}

	/**
	 * @return the coresLoad
	 */
	public ArrayList<Double> getCoresLoad() {
		return coresLoad;
	}

	/**
	 * @param coresLoad the coresLoad to set
	 */
	public void setCoresLoad(ArrayList<Double> coresLoad) {
		this.coresLoad = coresLoad;
	}


//	public HardwareAbstractionLayer getHal() {
//		return hal;
//	}
//
//	public void setHal(HardwareAbstractionLayer hal) {
//		this.hal = hal;
//	}
	
	
	//public static void main(String[] args) {

//		SystemStats ss = new SystemStats();
//		Thread t = new Thread(new StatsCollector(ss,2));
//		//t.setDaemon(true);
//		t.start();
		
	//}
}