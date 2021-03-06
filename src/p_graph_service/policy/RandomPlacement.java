package p_graph_service.policy;

import java.util.Random;
import java.util.Vector;

import p_graph_service.InstanceInfo;
import p_graph_service.PlacementPolicy;

public class RandomPlacement implements PlacementPolicy {
	private Random rand;
	private Vector<Long> inst;
	
	public RandomPlacement() {
		rand = new Random();
		inst = new Vector<Long>();
	}
	
	public RandomPlacement( long seed) {
		rand = new Random(seed);
		inst = new Vector<Long>();
	}

	@Override
	public void addInstance(long id, InstanceInfo inf) {
		inst.add(id);
	}

	@Override
	public long getPosition() {
		if(inst.isEmpty())return -1;
		int pos = rand.nextInt(inst.size());
		return inst.get(pos);
	}

	@Override
	public void removeInstance(long id) {
		inst.removeElement(id);
	}

}
