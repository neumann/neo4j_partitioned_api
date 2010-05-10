package p_graph_service.core;

import java.io.Serializable;
import java.util.HashMap;
import org.neo4j.graphdb.Relationship;

public class InstanceInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	public HashMap<Long, Long> globalTrafficMap;
	public HashMap<Long, Long> moveNodeMap;
	private long[] accesses; 
	
	public enum InfoKey{
		Glo_Traffic,  NumNodes, NumRelas, Loc_Traffic, rs_delete, n_delete, rs_create, n_create
	}
	
	public InstanceInfo() {
		this.globalTrafficMap = new HashMap<Long, Long>();
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
			accesses[InfoKey.Loc_Traffic.ordinal()]++;
			accesses[InfoKey.NumRelas.ordinal()]++;
			return;
		case rs_delete:
			accesses[InfoKey.Loc_Traffic.ordinal()]++;
			accesses[InfoKey.NumRelas.ordinal()]--;
			return;
		case n_create:
			accesses[InfoKey.Loc_Traffic.ordinal()]++;
			accesses[InfoKey.NumNodes.ordinal()]++;
			return;
		case n_delete:
			accesses[InfoKey.Loc_Traffic.ordinal()]++;
			accesses[InfoKey.NumNodes.ordinal()]--;
			return;
		default:
			break;
		}
	}
	
	public void resetTraffic(){
		accesses[InfoKey.Loc_Traffic.ordinal()] = 0;
		accesses[InfoKey.Glo_Traffic.ordinal()] = 0;
		this.globalTrafficMap = new HashMap<Long, Long>();
	}
	
	public String toString(){
		String res = "{";
		for (InfoKey k : InfoKey.values()) {
			res+="("+k.name() +" = " + accesses[k.ordinal()]+ ") ";
		}
		res+="}"+ globalTrafficMap;
		return res;
	}
	
	public InstanceInfo differenceTo(InstanceInfo info){
		InstanceInfo res = info.takeSnapshot();
		for(long id: this.globalTrafficMap.keySet()){
			long val = this.globalTrafficMap.get(id);
			if(res.globalTrafficMap.containsKey(id)){
				val = res.globalTrafficMap.get(id) - val;
			}else{
				val = -val;
			}
			res.globalTrafficMap.put(id, val);
		}
		for(int i = 0; i < accesses.length; i++ ){
			res.accesses[i] -=this.accesses[i];
		}
		return res;
	}
	
	@SuppressWarnings("unchecked")
	public InstanceInfo takeSnapshot(){
		InstanceInfo clone = new InstanceInfo();
		clone.globalTrafficMap = (HashMap<Long, Long>) this.globalTrafficMap.clone();
		clone.accesses = accesses.clone();
		return clone;
	}
	
	public void logHop(long[] pos, Relationship rs) {

		if (rs.hasProperty("_isGhost") || rs.hasProperty("_isHalf")) {
			// interhop on partitioned db
			accesses[InfoKey.Glo_Traffic.ordinal()]++;
			return;
		}
	}
	
	public void logInterComunication(byte to){
		long aim = to;
		accesses[InfoKey.Glo_Traffic.ordinal()]++;
		if(globalTrafficMap.containsKey(aim)){
			globalTrafficMap.put(aim, globalTrafficMap.get(aim)+1);
		}else{
			globalTrafficMap.put(aim, new Long(1));
		}
	}
}
