package p_graph_service.core;

import java.io.Serializable;
import java.util.HashMap;
import org.neo4j.graphdb.Relationship;

public class InstanceInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	public HashMap<Long, Long> interHopMap;
	private long[] accesses; 
	
	public enum InfoKey{
		InterHop, IntraHop, NumNodes, NumRelas, Traffic, rs_delete, n_delete, rs_create, n_create
	}
	
	public InstanceInfo() {
		this.interHopMap = new HashMap<Long, Long>();
		this.accesses = new long[InfoKey.values().length] ;
		for(int i=0; i<InfoKey.values().length ; i++){
			accesses[i]=0;
		}
	}
	
	public long getValue(InfoKey key){
		return accesses[key.ordinal()]++;
	}
	
	public void log( InfoKey key ){
		accesses[key.ordinal()]++;
		switch (key) {
		case rs_create:
			accesses[InfoKey.Traffic.ordinal()]++;
			accesses[InfoKey.NumRelas.ordinal()]++;
			return;
		case rs_delete:
			accesses[InfoKey.Traffic.ordinal()]++;
			accesses[InfoKey.NumRelas.ordinal()]--;
			return;
		case n_create:
			accesses[InfoKey.Traffic.ordinal()]++;
			accesses[InfoKey.NumNodes.ordinal()]++;
			return;
		case n_delete:
			accesses[InfoKey.Traffic.ordinal()]++;
			accesses[InfoKey.NumNodes.ordinal()]--;
			return;
		default:
			break;
		}
	}
	
	public void resetTraffic(){
		accesses[InfoKey.Traffic.ordinal()] = 0;
		accesses[InfoKey.InterHop.ordinal()] = 0;
		accesses[InfoKey.IntraHop.ordinal()] = 0;
		this.interHopMap = new HashMap<Long, Long>();
	}
	
	public String toString(){
		String res = "{";
		for (InfoKey k : InfoKey.values()) {
			res+="("+k.name() +" = " + accesses[k.ordinal()]+ ") ";
		}
		res+="}"+ interHopMap;
		return res;
	}
	
	public InstanceInfo differenceTo(InstanceInfo info){
		InstanceInfo res = info.takeSnapshot();
		for(long id: this.interHopMap.keySet()){
			long val = this.interHopMap.get(id);
			if(res.interHopMap.containsKey(id)){
				val = res.interHopMap.get(id) - val;
			}else{
				val = -val;
			}
			res.interHopMap.put(id, val);
		}
		for(int i = 0; i < accesses.length; i++ ){
			res.accesses[i] -=this.accesses[i];
		}
		return res;
	}
	
	@SuppressWarnings("unchecked")
	public InstanceInfo takeSnapshot(){
		InstanceInfo clone = new InstanceInfo();
		clone.interHopMap = (HashMap<Long, Long>) this.interHopMap.clone();
		clone.accesses = accesses.clone();
		return clone;
	}
	
	public void logHop(long[] pos, Relationship rs) {

		if (rs.hasProperty("_isGhost") || rs.hasProperty("_isHalf")) {
			// interhop on partitioned db
			accesses[InfoKey.InterHop.ordinal()]++;
			return;
		}

		// normal hop
		accesses[InfoKey.IntraHop.ordinal()]++;

	}

	public void logHop(byte pos, Relationship rs) {
		byte c1 = (Byte) rs.getStartNode().getProperty("_color");
		byte c2 = (Byte) rs.getEndNode().getProperty("_color");

		if (c1 == pos || c1 == c2) {
			accesses[InfoKey.IntraHop.ordinal()]++;
		} else {
			accesses[InfoKey.InterHop.ordinal()]++;
			long aim = c2;
			if(interHopMap.containsKey(aim)){
				interHopMap.put(aim, interHopMap.get(aim)+1);
			}else{
				interHopMap.put(aim, new Long(1));
			}
		}
	}	
}
