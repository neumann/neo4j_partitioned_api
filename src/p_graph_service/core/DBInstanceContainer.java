package p_graph_service.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Wrapper for EmbeddedGraphDatabase contains information on size
 * 
 * @author martin neumann
 */
public class DBInstanceContainer implements GraphDatabaseService {
	private final EmbeddedGraphDatabase db;
	private final static String storFileName  = "MetaInfo";
	
	// id of this instance
	private final long id;
	public long getID(){
		return id;
	}
	
	private InstanceInfo info;
	
	// traffic is define by: create Node, create Relationship, get Node, get Relationship,  delete Node, delete Relationship
	public void logTraffic(){
		info.traffic++;
	}
	
	private void logExtTraffic(long instanceID){
		if(info.interHopMap.containsKey(info.interHopMap)){
			long count = info.interHopMap.get(instanceID);
			count ++;
			info.interHopMap.put(instanceID, count);
		}else{
			info.interHopMap.put(instanceID, new Long(1));
		}
	}
	
	public void logAddNode(){
		info.numNodes++;
	}
	public void logRemNode(){
		info.numNodes--;
	}
	
	public void logAddRela(){
		info.numRelas++;
	}
	
	public void logRemRela(){
		info.numRelas--;
	}
	
	public void resetTraffic(){
		info.resetTraffic();
	}
		
	public DBInstanceContainer(String path, long id) {
		this.id = id;
		this.db = new EmbeddedGraphDatabase(path);
		this.info = new InstanceInfo();
		
		// load stored meta information
		try {
			InputStream fips = new FileInputStream(new File(path+"/"+storFileName));
			ObjectInputStream oips = new ObjectInputStream(fips);
			
			this.info = (InstanceInfo) oips.readObject();
		
			oips.close();
			fips.close();	
		} catch (Exception e) {
			// recalculate meta information if not found
			this.info = new InstanceInfo();
			
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
		info.traffic += info.numNodes;
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
		Relationship rs = db.getRelationshipById(id);
		info.intraHop ++;
		if(rs.hasProperty(Neo4jDB.IsHalf)){
			long[] adr = (long[]) rs.getProperty(Neo4jDB.IsHalf);
			logExtTraffic(adr[1]);
		}
		if(rs.hasProperty(Neo4jDB.IsGhost)){
			long[] adr = (long[]) rs.getProperty(Neo4jDB.IsGhost);
			logExtTraffic(adr[1]);
		}
		return rs;
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		return db.getRelationshipTypes();
	}

	@Override
	public void shutdown() {
		// store meta information
		try{
			OutputStream fops = new FileOutputStream(new File(db.getStoreDir()+"/"+storFileName));
			ObjectOutputStream oops = new ObjectOutputStream(fops);
			oops.writeObject(info);
			oops.close();
			fops.close();
		}
		catch (Exception e) {
			// nothing to do there
		}
		
		db.shutdown();
	}
	
	public InstanceInfo getInfo(){
		return (InstanceInfo) info.takeSnapshot();
	}

	@Override
	public KernelEventHandler registerKernelEventHandler(KernelEventHandler arg0) {
		return db.registerKernelEventHandler(arg0);
	}

	@Override
	public <T> TransactionEventHandler<T> registerTransactionEventHandler(
			TransactionEventHandler<T> arg0) {
		return db.registerTransactionEventHandler(arg0);
	}

	@Override
	public KernelEventHandler unregisterKernelEventHandler(
			KernelEventHandler arg0) {
		return db.unregisterKernelEventHandler(arg0);
	}

	@Override
	public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
			TransactionEventHandler<T> arg0) {
		return db.unregisterTransactionEventHandler(arg0);
	}

}
