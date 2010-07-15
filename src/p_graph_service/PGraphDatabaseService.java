package p_graph_service;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;


public interface PGraphDatabaseService extends GraphDatabaseService {
	// this service identifier
	public long getServiceID();

	// information on the stored instances
	public int getNumInstances();
	public long[] getInstancesIDs();
	
	// storage information
	public InstanceInfo getInstanceInfoFor(long id);
	public void resetLoggingOn(long id);
	
	// policy used when creating a node without specifying the target position
	public PlacementPolicy getPlacementPolicy();
	public void setPlacementPolicy(PlacementPolicy pol);
	
	// instance management
	public boolean addInstance();
	public boolean addInstance(long id);
	public boolean removeInstance(long id);
	// no implementation at the moment
	public boolean migrateInstance(String path, long id);
	
	// creating nodes with a certain GID or on a certain instance
	public Node createNode(long GID);
	public Node createNodeOn(long instanceID);
	public Node createNodeOn(long GID, long instanceID);
	
	// move a set of nodes to target instance
	public void moveNodes(Iterable<Node> nodes, long instanceID);
	
	// folder for read write log
	public void setDBChangeLog(String file);
	public String getDBChangeLog();
	
}
