package p_graph_service.core;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.PlacementPolicy;
import p_graph_service.policy.RandomPlacement;

public class PGraphDatabaseServiceImpl implements PGraphDatabaseService {
	private final long SERVICE_ID;
	private PlacementPolicy placementPol;

	// if no instance is found it is an empty container
	public PGraphDatabaseServiceImpl(String path, long id) {

		this.SERVICE_ID = id;

		// TODO put policy to a setting file
		this.placementPol = new RandomPlacement();

		Neo4jDB.startup(path);

		/*
		 * TODO bug in here need to be fixed // create reference node if not
		 * existing if( Inst.Lookup.findNode(0)== null &&
		 * !Inst.instances.isEmpty()){ long instID = this.getInstancesIDs()[0];
		 * this.createNode(0, instID); }
		 */

	}

	@Override
	public PlacementPolicy getPlacementPolicy() {
		return placementPol;
	}
	
	@Override
	public void setPlacementPolicy(PlacementPolicy pol) {
		this.placementPol = pol;
	}

	@Override
	public long getServiceID() {
		return SERVICE_ID;
	}
	
	@Override
	public boolean removeInstance(long id) {
		// TODO not tested will screw up reference node
		if (Neo4jDB.INST.get(id).getNumOfNodes() == 0) {
			Neo4jDB.INST.remove(id);
			return true;
		}
		return false;
	}

	@Override
	// creates a new DB-instance and allocates a ID
	public boolean addInstance() {
		return addInstance(System.currentTimeMillis());
	}

	@Override
	// loads an instance with the given path and ID
	// NOTE untested need to be rewritten since it might not work according to
	// concept either
	public boolean migrateInstance(String path, long id) {
		File f = new File(path);
		if (f.exists()) {
			DBInstanceContainer instContainer = new DBInstanceContainer(path,
					id);
			Neo4jDB.INST.put(id, instContainer);
			placementPol.addInstance(id, instContainer);
			return true;
		}
		return false;
	}
	
	@Override
	public long[] getInstancesIDs() {
		long[] res = new long[Neo4jDB.INST.keySet().size()];
		int i = 0;
		for (long id : Neo4jDB.INST.keySet()) {
			res[i] = id;
			i++;
		}
		return res;
	}

	@Override
	public int getNumInstances() {
		return Neo4jDB.INST.values().size();
	}

	@Override
	public Node createNode(long gid) {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		return createNodeOn(gid, placementPol.getPosition());
	}
	
	@Override
	public Node createNodeOn(long instanceID) {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		// create GID
		long gid = createGID();
		return createNodeOn(gid, instanceID);
	}
	
	@Override
	public Node createNode() {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		return createNode(placementPol.getPosition());
	}
	
	@Override
	// forms iterator internally to list so it can run out of heapspace
	public void moveNodes(Iterable<Node> partitionedNodes, long instanceID) {
		// storage
		Vector<Node> curNodes = new Vector<Node>();
		Vector<Node> aimNodes = new Vector<Node>();
		Vector<long[]> curPositions = new Vector<long[]>();
		Vector<long[]> aimPositions = new Vector<long[]>();

		// instance does not exist
		if (!Neo4jDB.INST.containsKey(instanceID))
			throw new Error("Instance " + instanceID + " does not exist");

		// transaction support
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(instanceID);

		// first move all nodes then repair all relations
		for (Node curN : partitionedNodes) {

			// find current position and nodeGID
			// NOTE if nodes happen to be on an other server it will crash
			long nodeGID = ((PNode) curN).getId();
			long[] curPos = ((PNode) curN).getPos();
			curN = ((PNode) curN).getWrappedNode();

			// is already on the right instance
			if (curPos[1] == instanceID)
				continue;

			// -------------- move node ------------------------------

			// find GNode on targetInstance
			Node aimN = Neo4jDB.findGhostForNode(curN, instanceID);
			// create a new node if none has been found
			if (aimN == null) {
				aimN = Neo4jDB.INST.get(instanceID).createNode();
				aimN.setProperty(Neo4jDB.nGID, nodeGID);
			} else {
				// make aim node a none ghost
				aimN.removeProperty(Neo4jDB.IsGhost);
			}

			// copy all properties
			for (String key : curN.getPropertyKeys()) {
				aimN.setProperty(key, curN.getProperty(key));
			}
			// set curNode node as ghost node for the aim node
			long[] aimPos = new long[3];
			aimPos[0] = curPos[0];
			aimPos[1] = instanceID;
			aimPos[2] = aimN.getId();
			curN.setProperty(Neo4jDB.IsGhost, aimPos);

			// update lookup and instance information
			Neo4jDB.INDEX.addNode(nodeGID, aimPos);
			Neo4jDB.INST.get(curPos[1]).logRemNode();
			Neo4jDB.INST.get(aimPos[1]).logAddNode();

			// store information to reuse on repairing relations if needed
			if (curN.hasRelationship()) {
				curPositions.add(curPos);
				aimPositions.add(aimPos);
				curNodes.add(curN);
				aimNodes.add(aimN);
			} else {
				curN.delete();
			}
		}
		// nodes to be deleted in the end
		HashSet<Node> nodeToDelete = new HashSet<Node>();

		// repairing relationships
		for (int i = 0; i < curNodes.size(); i++) {

			// load from storage
			Node curN = curNodes.get(i);
			Node aimN = aimNodes.get(i);
			long[] curPos = curPositions.get(i);
			long[] aimPos = aimPositions.get(i);

			// repair incoming relations
			for (Relationship rs : curN.getRelationships(Direction.INCOMING)) {
				Node sNode = rs.getStartNode();

				// its a ghost relation
				if (rs.hasProperty(Neo4jDB.IsGhost)) {

					// get half relation information
					long[] hRelPos = (long[]) rs.getProperty(Neo4jDB.IsGhost);
					Relationship hRel = Neo4jDB.INST.get(hRelPos[1])
							.getRelationshipById(hRelPos[2]);
					Node hRelStartNode = hRel.getStartNode();

					// Start node is a normal node
					if (!hRelStartNode.hasProperty(Neo4jDB.IsGhost)) {

						// move to the same instance so ghost construct is not
						// needed anymore
						if (instanceID == hRelPos[1]) {
							hRel.removeProperty(Neo4jDB.IsHalf);
							rs.delete();
						}

						// ghost construct need to be upgraded
						else {
							// find new start node for ghost relationship if
							// existing
							Node newGRelStartNode = null;
							newGRelStartNode = Neo4jDB.findGhostForNode(
									hRelStartNode, instanceID);
							// create ghost node if not existing
							if (newGRelStartNode == null) {
								newGRelStartNode = Neo4jDB.INST.get(instanceID)
										.createNode();
								newGRelStartNode.setProperty(Neo4jDB.nGID, sNode
										.getProperty(Neo4jDB.nGID));
								newGRelStartNode.setProperty(Neo4jDB.IsGhost,
										sNode.getProperty(Neo4jDB.IsGhost));
							}

							// create the relationships and link to half
							// relation
							Relationship newRS = newGRelStartNode
									.createRelationshipTo(aimN, rs.getType());
							newRS.setProperty(Neo4jDB.rGID, rs
									.getProperty(Neo4jDB.rGID));
							newRS.setProperty(Neo4jDB.IsGhost, rs
									.getProperty(Neo4jDB.IsGhost));

							// update half relation and its endNode
							long[] newRSPos = new long[3];
							newRSPos[0] = aimPos[0];
							newRSPos[1] = aimPos[1];
							newRSPos[2] = newRS.getId();
							hRel.setProperty(Neo4jDB.IsHalf, newRSPos);
							hRel.getEndNode().setProperty(Neo4jDB.IsGhost,
									aimPos);

							rs.delete();
						}
					}
				}
				// its a normal relation and start node has not been moved
				else if (!sNode.hasProperty(Neo4jDB.IsGhost)) {

					long[] sNodePos = new long[3];
					sNodePos[0] = curPos[0];
					sNodePos[1] = curPos[1];
					sNodePos[2] = sNode.getId();

					// find the new Start node or create it if not existing
					Node newGRelStartNode = Neo4jDB.findGhostForNode(sNode,
							instanceID);
					if (newGRelStartNode == null) {
						newGRelStartNode = Neo4jDB.INST.get(instanceID)
								.createNode();
						newGRelStartNode.setProperty(Neo4jDB.nGID, sNode
								.getProperty(Neo4jDB.nGID));
						newGRelStartNode.setProperty(Neo4jDB.IsGhost, sNodePos);
					}

					// create the ghost relation and link it to half relation
					Relationship newRS = newGRelStartNode.createRelationshipTo(
							aimN, rs.getType());
					newRS.setProperty(Neo4jDB.rGID, rs.getProperty(Neo4jDB.rGID));
					long[] hRelPos = new long[3];
					hRelPos[0] = curPos[0];
					hRelPos[1] = curPos[1];
					hRelPos[2] = rs.getId();
					newRS.setProperty(Neo4jDB.IsGhost, hRelPos);

					// update half relation
					long[] newRSPos = new long[3];
					newRSPos[0] = aimPos[0];
					newRSPos[1] = aimPos[1];
					newRSPos[2] = newRS.getId();
					rs.setProperty(Neo4jDB.IsHalf, newRSPos);
				}
				// start and end node will be moved
				// relationship will be repaired by the start node
				else {
					continue;
				}

				if (!sNode.hasRelationship()
						&& sNode.hasProperty(Neo4jDB.IsGhost)) {
					nodeToDelete.add(sNode);
				}
			}

			for (Relationship rs : curN.getRelationships(Direction.OUTGOING)) {
				Node eNode = rs.getEndNode();
				long[] newRsPos;

				if (rs.hasProperty(Neo4jDB.IsHalf)) {
					// gather information on ghost relation
					long[] gRelPos = (long[]) rs.getProperty(Neo4jDB.IsHalf);
					Relationship gRel = Neo4jDB.INST.get(gRelPos[1])
							.getRelationshipById(gRelPos[2]);
					Node gRelEndNode = gRel.getEndNode();

					// both nodes have been moved to the same partition no ghost
					// construct needed anymore
					if (gRelEndNode.hasProperty(Neo4jDB.IsGhost)) {

						long[] newEndNodePos = (long[]) gRelEndNode
								.getProperty(Neo4jDB.IsGhost);
						Node newEndNode = Neo4jDB.INST.get(newEndNodePos[1])
								.getNodeById(newEndNodePos[2]);

						Relationship newRs = aimN.createRelationshipTo(
								newEndNode, rs.getType());
						newRsPos = new long[3];
						newRsPos[0] = aimPos[0];
						newRsPos[1] = aimPos[1];
						newRsPos[2] = newRs.getId();

						// copy relationship
						rs.removeProperty(Neo4jDB.IsHalf);
						for (String key : rs.getPropertyKeys()) {
							newRs.setProperty(key, rs.getProperty(key));
						}
						rs.delete();
						gRel.delete();
					}
					// start node moves to the same partition as the end node no
					// ghost construct needed anymore
					else if (gRelPos[1] == instanceID) {
						newRsPos = gRelPos;
						rs.removeProperty(Neo4jDB.IsHalf);
						gRel.removeProperty(Neo4jDB.IsGhost);

						// copy relationship
						for (String key : rs.getPropertyKeys()) {
							gRel.setProperty(key, rs.getProperty(key));
						}

						rs.delete();
					}
					// after moving nodes are still on different partitions ->
					// update ghost construct
					else {
						// find the new end node or create it if not existing
						long[] eNodePos = (long[]) eNode
								.getProperty(Neo4jDB.IsGhost);
						Node newENode = Neo4jDB.findGhostForNode(Neo4jDB.INST
								.get(eNodePos[1]).getNodeById(eNodePos[2]),
								instanceID);
						if (newENode == null) {
							newENode = Neo4jDB.INST.get(instanceID)
									.createNode();
							newENode.setProperty(Neo4jDB.nGID, eNode
									.getProperty(Neo4jDB.nGID));
							newENode.setProperty(Neo4jDB.IsGhost, eNode
									.getProperty(Neo4jDB.IsGhost));
						}
						// create the relationship and link to ghost relation
						Relationship newRS = aimN.createRelationshipTo(
								newENode, rs.getType());
						for (String key : rs.getPropertyKeys()) {
							newRS.setProperty(key, rs.getProperty(key));
						}

						// update ghost relation and startNode
						newRsPos = new long[3];
						newRsPos[0] = aimPos[0];
						newRsPos[1] = aimPos[1];
						newRsPos[2] = newRS.getId();
						gRel.setProperty(Neo4jDB.IsGhost, newRsPos);
						gRel.getStartNode()
								.setProperty(Neo4jDB.IsGhost, aimPos);

						rs.delete();
					}

				}
				// normal relationship
				else {
					long[] eNodePos = new long[3];
					eNodePos[0] = curPos[0];
					eNodePos[1] = curPos[1];
					eNodePos[2] = eNode.getId();

					// both nodes will be moved to same partition
					if (eNode.hasProperty(Neo4jDB.IsGhost)) {
						long[] newENodePos = (long[]) eNode
								.getProperty(Neo4jDB.IsGhost);
						Node newENode = Neo4jDB.INST.get(newENodePos[1])
								.getNodeById(newENodePos[2]);
						Relationship newRS = aimN.createRelationshipTo(
								newENode, rs.getType());
						rs.removeProperty(Neo4jDB.IsHalf);
						for (String key : rs.getPropertyKeys()) {
							newRS.setProperty(key, rs.getProperty(key));
						}
						newRsPos = new long[3];
						newRsPos[0] = aimPos[0];
						newRsPos[1] = aimPos[1];
						newRsPos[2] = newRS.getId();

						rs.delete();
						if (!eNode.hasRelationship()) {
							nodeToDelete.add(eNode);
						}
					}
					// start node moves to new instance so ghost construct need
					// to be created
					else {
						// find the new Start node or create it if not existing
						Node newENode = Neo4jDB.findGhostForNode(eNode,
								instanceID);
						if (newENode == null) {
							newENode = Neo4jDB.INST.get(instanceID)
									.createNode();
							newENode.setProperty(Neo4jDB.nGID, eNode
									.getProperty(Neo4jDB.nGID));
							newENode.setProperty(Neo4jDB.IsGhost, eNodePos);
						}
						// create the half relation and link it to ghost
						// relation
						Relationship newRS = aimN.createRelationshipTo(
								newENode, rs.getType());
						for (String key : rs.getPropertyKeys()) {
							newRS.setProperty(key, rs.getProperty(key));
						}
						long[] gRsPos = new long[3];
						gRsPos[0] = curPos[0];
						gRsPos[1] = curPos[1];
						gRsPos[2] = rs.getId();
						newRS.setProperty(Neo4jDB.IsHalf, gRsPos);
						newRsPos = new long[3];
						newRsPos[0] = aimPos[0];
						newRsPos[1] = aimPos[1];
						newRsPos[2] = newRS.getId();

						// make normal relation to ghost relation
						rs.setProperty(Neo4jDB.IsGhost, newRsPos);
					}
				}

				// update lookup
				Neo4jDB.INDEX.addRela((Long) rs.getProperty(Neo4jDB.rGID),
						newRsPos);
				Neo4jDB.INST.get(newRsPos[1]).logAddRela();
				Neo4jDB.INST.get(curPos[1]).logAddRela();

				// mark node for deletion if not needed
				if (!eNode.hasRelationship()
						&& eNode.hasProperty(Neo4jDB.IsGhost)) {
					nodeToDelete.add(eNode);
				}
			}
			// mark for deletion if not needed anymore
			if (!curN.hasRelationship()) {
				nodeToDelete.add(curN);
			}
		}

		// delete unused nodes
		for (Node n : nodeToDelete) {
			n.delete();
		}

		// increase version number of the graph to force all references to
		// update
		Neo4jDB.VERS++;
	}

	

	@Override
	public Transaction beginTx() {
		if (Neo4jDB.PTX == null) {
			Neo4jDB.PTX = new PTransaction();
		}
		return Neo4jDB.PTX;
	}

	

	@Override
	public boolean enableRemoteShell() {
		throw new UnsupportedOperationException(
				"PGraphDBService.enableRemoteShell() not implemented");
	}

	@Override
	public boolean enableRemoteShell(Map<String, Serializable> initialProperties) {
		throw new UnsupportedOperationException(
				"PGraphDBService.enableRemoteShell(Map<String, Serializable> initialProperties) not implemented");
	}

	@Override
	public Iterable<Node> getAllNodes() {
		// throw Exception if not wrapped in transaction
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		// create transactions if not yet existing
		for (Long containerID : Neo4jDB.INST.keySet()) {
			Neo4jDB.PTX.registerResource(containerID);
		}

		HashSet<Iterable<Node>> iterables = new HashSet<Iterable<Node>>();
		for (DBInstanceContainer db : Neo4jDB.INST.values()) {
			iterables.add(db.getAllNodes());
		}
		return new JoinedIterable(iterables);
	}

	@Override
	public Node getNodeById(long id) {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		long[] adr = Neo4jDB.INDEX.findNode(id);
		if (adr[0] == getServiceID() && Neo4jDB.INST.containsKey(adr[1])) {
			// create transaction if not yet existing
			Neo4jDB.PTX.registerResource(adr[1]);
			return new PNode(Neo4jDB.INST.get(adr[1]).getNodeById(adr[2]));
		}
		return null;
	}

	@Override
	public Node getReferenceNode() {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		return this.getNodeById(0);
	}

	@Override
	public Relationship getRelationshipById(long id) {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		long[] adr = Neo4jDB.INDEX.findRela(id);
		if (adr[0] == getServiceID() && Neo4jDB.INST.containsKey(adr[1])) {
			// create transaction if not yet existing
			Neo4jDB.PTX.registerResource(adr[1]);
			return Neo4jDB.INST.get(adr[1]).getRelationshipById(adr[2]);
		}
		return null;
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();

		// create transactions if not yet existing
		for (Long containerID : Neo4jDB.INST.keySet()) {
			Neo4jDB.PTX.registerResource(containerID);
		}

		// TODO prevent duplicates in the result
		HashSet<Iterable<RelationshipType>> iterables = new HashSet<Iterable<RelationshipType>>();
		for (DBInstanceContainer db : Neo4jDB.INST.values()) {
			iterables.add(db.getRelationshipTypes());
		}
		return null;
	}

	@Override
	public void shutdown() {
		// shutdown instances
		for (DBInstanceContainer dbInst : Neo4jDB.INST.values()) {
			dbInst.shutdown();
		}

		// shutdown lookup
		Neo4jDB.INDEX.shutdown();

		// shutdown GIDcreator
		Neo4jDB.GIDGenNode.close();
		Neo4jDB.GIDGenRela.close();
	}

	// TODO refine to make joining of the id's nicer
	private long createGID() {
		long localPart = Neo4jDB.GIDGenNode.nextId();
		String gid = SERVICE_ID + "" + localPart;
		return Long.parseLong(gid);
	}

	// utility class that joins several Iterable classes together
	class JoinedIterable implements Iterable<Node> {
		Set<Iterable<Node>> joined;
		String type;

		public JoinedIterable(Set<Iterable<Node>> joined) {
			this.joined = joined;
		}

		@Override
		public Iterator<Node> iterator() {
			if (joined == null)
				return new JoinedIterator(null);
			Set<Iterator<Node>> iterSet = new HashSet<Iterator<Node>>();
			for (Iterable<Node> t : joined) {
				iterSet.add(t.iterator());
			}
			return new JoinedIterator(iterSet);
		}
	}

	// iterator on JoinedIterable.class
	class JoinedIterator implements Iterator<Node> {
		Set<Iterator<Node>> joined;
		Iterator<Node> curIter = null;
		Node item = null;

		public JoinedIterator(Set<Iterator<Node>> joined) {
			this.joined = joined;
		}

		@Override
		public boolean hasNext() {
			Iterator<Iterator<Node>> iter = joined.iterator();
			while (item == null && !joined.isEmpty()) {

				while (curIter == null || !curIter.hasNext()) {
					if (!iter.hasNext()) {
						return false;
					}
					curIter = iter.next();
					if (!curIter.hasNext()) {
						curIter = null;
					}
				}

				if (curIter != null) {
					item = curIter.next();
					if (item.getId() == 0 || item.hasProperty(Neo4jDB.IsGhost)) {
						item = null;
					}
				}
			}

			return (item != null);
		}

		@Override
		public Node next() {
			if (hasNext()) {
				PNode res = new PNode(item);
				item = null;
				return res;

			}
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"JoinedIterator.remove() not implemented");
		}
	}

	@Override
	public Node createNodeOn(long gid, long instanceID) {
		if (Neo4jDB.PTX == null)
			throw new NotInTransactionException();
		DBInstanceContainer inst = Neo4jDB.INST.get(instanceID);

		// create transaction if not existing
		Neo4jDB.PTX.registerResource(instanceID);

		// create node
		Node n = null;
		n = inst.createNode();
		n.setProperty(Neo4jDB.nGID, gid);

		// register node to lookup service
		long[] adr = { this.getServiceID(), instanceID, n.getId() };
		Neo4jDB.INDEX.addNode(gid, adr);
		Neo4jDB.INST.get(adr[1]).logAddNode();

		return new PNode(n);
	}
	
	@Override
	public void createDistribution(String db) {
		ArrayList<Long> nodeIDs = new ArrayList<Long>();
		GraphDatabaseService dbS = new EmbeddedGraphDatabase(db);
		
		// load all instance ids
		HashSet<Long> instIDs = new HashSet<Long>();
		for (Long instID : getInstancesIDs()) {
			instIDs.add(instID);
		}
		
		
		System.out.println(new Date(System.currentTimeMillis())+" count nodes");
		
		// counts all nodes
		Transaction tx = dbS.beginTx();
		try {
			for(Node n : dbS.getAllNodes()){
				// ignore reference node
				if (n.getId() == 0)
					continue;
				
				nodeIDs.add(n.getId());
			}
			tx.success();
		} finally {
			tx.finish();
		}
		
		int nodesInSystem = nodeIDs.size();
		int nodeCount;
		int stepSize = 1000;
		int stepCount;
		
		
		Iterator<Long>idIter;
		
		System.out.println(new Date(System.currentTimeMillis())+ " creating nodes");
		
		nodeCount = 0;
		stepCount = 0;
		idIter = nodeIDs.iterator();
		while(idIter.hasNext()){
			tx = dbS.beginTx();
			try {
				// my own transaction
				Transaction pTx = beginTx();
				try {
					while(idIter.hasNext() && stepCount < stepSize){
						Node n = dbS.getNodeById(idIter.next());
						
						long targetInst = (Byte) n.getProperty("_color");
						long gid = (Long) n.getProperty("_n_gid");

						// create instance if not yet existing
						if (!instIDs.contains(targetInst)) {
							addInstance(targetInst);
							instIDs.add(targetInst);
						}
						Node newN = createNodeOn(gid, targetInst);

						for (String key : n.getPropertyKeys()) {
							newN.setProperty(key, n.getProperty(key));
						}
						
						stepCount++;
						nodeCount++;
					}
					stepCount = 0;
					System.out.println(new Date(System.currentTimeMillis())+" " + nodeCount+ " of "+ nodesInSystem + " created");
					pTx.success();
				} finally {
					pTx.finish();
				}	
				tx.success();
			} finally {
				tx.finish();
			}
		}
		
		System.out.println(new Date(System.currentTimeMillis())+" create relationships");
		nodeCount = 0;
		stepCount = 0;
		idIter = nodeIDs.iterator();
		while(idIter.hasNext()){
			tx = dbS.beginTx();
			try {
				// my own transaction
				Transaction pTx = beginTx();
				try {
					while(idIter.hasNext() && stepCount < stepSize){
						Node n = dbS.getNodeById(idIter.next());
						
						long curN = (Long) n.getProperty("_n_gid");
						Node srtNode = getNodeById(curN);

						for (Relationship rs : n
								.getRelationships(Direction.OUTGOING)) {

							long endNodeGID = (Long) rs
									.getEndNode().getProperty("_n_gid");
							Node endNode = getNodeById(endNodeGID);
							Relationship newRs = srtNode.createRelationshipTo(
									endNode, rs.getType());
							// set the GID to the relationship LID 
							//Does not work anway
							//newRs.setProperty(Neo4jDB.rGID, rs.getProperty("_r_gid"));

							// copy all properties
							for (String key : rs.getPropertyKeys()) {
								newRs.setProperty(key, rs.getProperty(key));
							}
						}						
						stepCount++;
						nodeCount++;
					}
					stepCount = 0;
					System.out.println(new Date(System.currentTimeMillis())+" relationship for " +nodeCount+ " of "+ nodesInSystem + " created");
					pTx.success();
				} finally {
					pTx.finish();
				}	
				tx.success();
			} finally {
				tx.finish();
			}
		}
		System.out.println("done");
		dbS.shutdown();
	
	}

	@Override
	public boolean addInstance(long id) {
		String folder = Neo4jDB.DB_DIR.getAbsolutePath() + "/" + "instance"
				+ id;
		DBInstanceContainer instContainer = new DBInstanceContainer(folder, id);
		// delete reference node
		Transaction tx = instContainer.beginTx();
		try {
			instContainer.getNodeById(0).delete();
			tx.success();
		} finally {
			tx.finish();
		}
		Neo4jDB.INST.put(id, instContainer);
		placementPol.addInstance(id, instContainer);
		return true;
	}

	@Override
	public long getNumNodes() {
		long res = 0;
		for (long key : Neo4jDB.INST.keySet()) {
			res += Neo4jDB.INST.get(key).getNumOfNodes();
		}
		return res;
	}

	@Override
	public long getNumNodesOn(long id) {
		return Neo4jDB.INST.get(id).getNumOfNodes();
	}

	@Override
	public long getNumRelations() {
		long res = 0;
		for (long key : Neo4jDB.INST.keySet()) {
			res += Neo4jDB.INST.get(key).getNumOfRelas();
		}
		return res;
	}

	@Override
	public long getNumRelationsOn(long id) {
		return Neo4jDB.INST.get(id).getNumOfRelas();
	}

	@Override
	public long getTrafficOn(long id) {
		return Neo4jDB.INST.get(id).getTraffic();
	}

}
