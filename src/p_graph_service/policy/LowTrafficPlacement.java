package p_graph_service.policy;

import java.util.HashMap;

import p_graph_service.PlacementPolicy;
import p_graph_service.core.DBInstanceContainer;

public class LowTrafficPlacement implements PlacementPolicy {
	private HashMap<Long, DBInstanceContainer> inst;
	
	public LowTrafficPlacement() {
		inst = new HashMap<Long, DBInstanceContainer>();
	}

	@Override
	public void addInstance(long id, DBInstanceContainer db) {
		inst.put(id, db);
	}

	@Override
	public long getPosition() {
		long posLow = -1;
		long value = Long.MAX_VALUE;
		
		for(long key :inst.keySet()){
			long numNodes = inst.get(key).getTraffic();
			if(numNodes < value){
				value = numNodes;
				posLow = key;
			}
		}
		return posLow;
		
	}

	@Override
	public void removeInstance(long id) {
		inst.remove(id);	
	}

}
