package p_graph_service.core;

import java.io.Serializable;
import java.util.Map;

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
	private final long id;
	
	// internal information on the DB
	private long numNodes;
	
	
	
	public long getID(){
		return id;
	}
	
	public long getNumberOfNodes(){
		return numNodes;
	}
	
	public DBInstanceContainer(String path, long id) {
		this.id = id;
		this.db = new EmbeddedGraphDatabase(path);
		this.numNodes = 0;
	}
	
	@Override
	public Transaction beginTx() {
		return db.beginTx();
	}

	@Override
	public Node createNode() {
		numNodes++;
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
		return db.getAllNodes();
	}

	@Override
	public Node getNodeById(long id) {
		return db.getNodeById(id);
	}

	@Override
	public Node getReferenceNode() {
		return db.getReferenceNode();
	}

	@Override
	public Relationship getRelationshipById(long id) {
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
