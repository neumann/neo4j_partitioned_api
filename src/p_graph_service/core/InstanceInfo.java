package p_graph_service.core;

import java.util.HashMap;

public class InstanceInfo implements Cloneable {
	protected long numNodes;
	protected long numRelas;
	protected long traffic;
	protected long interHop;
	protected long intraHop;
	protected HashMap<Long, Long> interHopMap;
	
	InstanceInfo() {
		this.numNodes = 0;
		this.numRelas = 0;
		
		this.traffic = 0;
		this.interHop = 0;
		this.intraHop = 0;
		this.interHopMap = new HashMap<Long, Long>();
	}
	
	protected void resetTraffic(){
		this.traffic = 0;
		this.interHop = 0;
		this.intraHop = 0;
		this.interHopMap = new HashMap<Long, Long>();
	}
	
	public long getNumNodes() {
		return numNodes;
	}
	public long getNumRelas() {
		return numRelas;
	}
	public long getTraffic() {
		return traffic;
	}
	public long getInterHop() {
		return interHop;
	}
	public long getIntraHop() {
		return intraHop;
	}
	@SuppressWarnings("unchecked")
	public HashMap<Long, Long> getInterHopMap() {
		return (HashMap<Long, Long>) interHopMap.clone();
	}
	
	public String toString(){
		return "not implemented yet";
	}
	
	public InstanceInfo compareTo(InstanceInfo shot){
		InstanceInfo instInf = (InstanceInfo) shot.clone();
		
		instInf.numNodes -= this.numNodes;
		instInf.numRelas -= this.numRelas;
		
		instInf.traffic -= this.traffic;
		instInf.interHop -= this.interHop;
		instInf.intraHop -= this.intraHop;
		for(long k :this.interHopMap.keySet()){
			long val = this.interHopMap.get(k);
			if(instInf.interHopMap.containsKey(k)){
				instInf.interHopMap.put(k, instInf.interHopMap.get(k)-val);
			}else{
				instInf.interHopMap.put(k, -val);
			}
		}
		
		return instInf;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected Object clone() {
		InstanceInfo clone = new InstanceInfo();
		clone.numNodes = this.numNodes;
		clone.numRelas =  this.numRelas;
		clone.traffic =  this.traffic;
		clone.interHop = this.interHop;
		clone.intraHop = this.intraHop;
		clone.interHopMap = (HashMap<Long, Long>) this.interHopMap.clone();
		return clone;
	}
	

}
