package p_graph_service.policy;

import java.util.HashMap;

import p_graph_service.InstanceInfo;
import p_graph_service.PlacementPolicy;
import p_graph_service.InstanceInfo.InfoKey;

public class LowNodecountPlacement implements PlacementPolicy {
	private HashMap<Long, InstanceInfo> inst;
	
	public LowNodecountPlacement() {
		inst = new HashMap<Long, InstanceInfo>();
	}

	@Override
	public void addInstance(long id, InstanceInfo inf) {
		inst.put(id, inf);
	}

	@Override
	public long getPosition() {
		long posLow = -1;
		long value = Long.MAX_VALUE;
		
		for(long key :inst.keySet()){
			long numNodes = inst.get(key).getValue(InfoKey.NumNodes);
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
