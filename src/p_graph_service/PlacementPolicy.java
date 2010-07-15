package p_graph_service;


public interface PlacementPolicy {
	public void addInstance(long id, InstanceInfo inf);
	public void removeInstance(long id);
	public long getPosition();
}
