package p_graph_service;

/**
 * Interface for GlobalID index service. Stores global unique id of nodes and relationships as well as there location.
 * 
 * 
 * @author martin neumann
 */
public interface GIDLookup  {
	public void addNode(long gid, long[] pos);
	public void addRela(long gid, long[] pos);
	
	public void remNode(long gid);
	public void remRela(long gid);
	
	public long[] findNode(long gid);
	public long[] findRela(long gid);
	
	public void shutdown();
}
