package p_graph_service;

import p_graph_service.core.InstanceInfo;

public interface PlacementPolicy {
	public void addInstance(long id, InstanceInfo inf);
	public void removeInstance(long id);
	public long getPosition();
}
