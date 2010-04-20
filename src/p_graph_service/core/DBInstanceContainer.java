package p_graph_service.core;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Wrapper for EmbeddedGraphDatabase contains information on size
 * 
 * @author martin neumann
 */
public class DBInstanceContainer implements GraphDatabaseService {
	private final EmbeddedGraphDatabase db;
	
	// id of this instance
	private final long id;
	public long getID(){
		return id;
	}
	
	// internal information on the DB
	private long numNodes;
	private long numRelas;
	private long traffic;
	
	// traffic is define by: create Node, create Relationship, get Node, get Relationship,  delete Node, delete Relationship
	public void logTraffic(){
		traffic++;
	}
	public void logAddNode(){
		numNodes++;
	}
	public void logRemNode(){
		numNodes--;
	}
	
	public void logAddRela(){
		numRelas++;
	}
	
	public void logRemRela(){
		numRelas--;
	}
	
	
	public long getNumOfNodes(){
		return numNodes;
	}
	public long getNumOfRelas(){
		return numRelas;
	}
	public long getTraffic(){
		return traffic;
	}
	
	public DBInstanceContainer(String path, long id) {
		this.id = id;
		this.db = new EmbeddedGraphDatabase(path);
		this.numNodes = 0;
		this.traffic = 0;
		this.numRelas = 0;
		
		// count relationships and nodes (ghosts and entities without GID are excluded)
		Transaction tx = beginTx();
		try {
			for(Node n :  getAllNodes()){
				if(n.hasProperty(Neo4jDB.nGID) && !n.hasProperty(Neo4jDB.IsGhost)){
					logAddNode();
					for(Relationship r : n.getRelationships(Direction.OUTGOING)){
						if(r.hasProperty(Neo4jDB.rGID) && !r.hasProperty(Neo4jDB.IsGhost)){
							logAddRela();
						}
					}
				}
			}
			
			tx.success();
		} finally {
			tx.finish();
		}
	}
	
	@Override
	public Transaction beginTx() {
		return db.beginTx();
	}

	@Override
	public Node createNode() {
		logTraffic();
		return db.createNode();
	}

	@Override
	public boolean enableRemoteShell() {
		throw new UnsupportedOperationException("DBInstanceContainer.enableRemoteShell() not implemented");
	}

	@Override
	public boolean enableRemoteShell(Map<String, Serializable> initialProperties) {
		throw new UnsupportedOperationException("DBInstanceContainer.enableRemoteShell(Map<String, Serializable> initialProperties) not implemented");
	}

	@Override
	public Iterable<Node> getAllNodes() {
		traffic += numNodes;
		return db.getAllNodes();
	}

	@Override
	public Node getNodeById(long id) {
		logTraffic();
		return db.getNodeById(id);
	}

	@Override
	public Node getReferenceNode() {
		logTraffic();
		return db.getReferenceNode();
	}

	@Override
	public Relationship getRelationshipById(long id) {
		logTraffic();
		return db.getRelationshipById(id);
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		return db.getRelationshipTypes();
	}

	@Override
	public void shutdown() {
		db.shutdown();

	}

}
