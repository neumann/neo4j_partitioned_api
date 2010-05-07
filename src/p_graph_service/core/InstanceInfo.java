package p_graph_service.core;

import java.io.Serializable;
import java.util.HashMap;

public class InstanceInfo implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public long numNodes;
	public long numRelas;
	public long traffic;
	public long interHop;
	public long intraHop;
	public HashMap<Long, Long> interHopMap;
	
	public InstanceInfo() {
		this.numNodes = 0;
		this.numRelas = 0;
		
		this.traffic = 0;
		this.interHop = 0;
		this.intraHop = 0;
		this.interHopMap = new HashMap<Long, Long>();
	}
	
	public void resetTraffic(){
		this.traffic = 0;
		this.interHop = 0;
		this.intraHop = 0;
		this.interHopMap = new HashMap<Long, Long>();
	}
	
	public String toString(){
		return "not implemented yet";
	}
	
	public InstanceInfo differenceTo(InstanceInfo info){
		InstanceInfo res = info.takeSnapshot();
		res.interHop -= this.interHop;
		res.intraHop -= this.intraHop;
		res.numNodes -= this.numNodes;
		res.numRelas -= this.numRelas;
		res.traffic  -= this.traffic;
		for(long id: this.interHopMap.keySet()){
			long val = this.interHopMap.get(id);
			if(res.interHopMap.containsKey(id)){
				val = res.interHopMap.get(id) - val;
			}else{
				val = -val;
			}
			res.interHopMap.put(id, val);
		}
		
		return res;
	}
	
	@SuppressWarnings("unchecked")
	public InstanceInfo takeSnapshot(){
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
