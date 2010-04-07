package p_graph_service;

import p_graph_service.core.DBInstanceContainer;

public interface PlacementPolicy {
	public void addInstance(long id, DBInstanceContainer db);
	public void removeInstance(long id);
	public long getPosition();
}
