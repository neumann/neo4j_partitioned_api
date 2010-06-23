package p_graph_service.core;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expansion;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

import p_graph_service.GIDLookup;
import p_graph_service.PConst;
import p_graph_service.PGraphDatabaseService;
import p_graph_service.PlacementPolicy;
import p_graph_service.core.InstanceInfo.InfoKey;
import p_graph_service.policy.RandomPlacement;

public class PGraphDatabaseServiceImpl implements PGraphDatabaseService {
	private final long SERVICE_ID;
	private PlacementPolicy placementPol;

	protected long VERS;
	// neo4j instances
	public HashMap<Long, DBInstanceContainer> INST;
	// berkley db lookup service
	protected GIDLookup INDEX;
	// GID generator (basically copy of the neo4j version)
	protected IdGenerator GIDGenRela;
	protected IdGenerator GIDGenNode;
	// db folder
	protected File DB_DIR;
	// transaction support
	protected PTransaction PTX = null;

	private Node findGhostForNode(Node node, long instance) {
		Node res = null;

		Iterator<Relationship> it = node.getRelationships().iterator();
		while (it.hasNext() && res == null) {
			Relationship rs = it.next();
			if (rs.hasProperty(PConst.IsHalf)) {
				long[] pos = (long[]) rs.getProperty(PConst.IsHalf);
				if (pos[1] == instance) {
					res = INST.get(pos[1]).getRelationshipById(pos[2])
							.getStartNode();
				}
			} else if (rs.hasProperty(PConst.IsGhost)) {
				long[] pos = (long[]) rs.getProperty(PConst.IsGhost);
				if (pos[1] == instance) {
					res = INST.get(pos[1]).getRelationshipById(pos[2])
							.getEndNode();
				}
			}
		}
		return res;
	}

	private void startup(String path) {
		this.VERS = 0;
		this.INST = new HashMap<Long, DBInstanceContainer>();

		// load instances
		this.DB_DIR = new File(path);

		// create folder is not existent
		if (!DB_DIR.exists()) {
			DB_DIR.mkdirs();
		}

		// list DBinstances
		File[] instances = DB_DIR.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isDirectory() && file.getName().matches(
						PConst.InstaceRegex));
			}
		});

		// create found instances
		for (File inst : instances) {
			long instID = Long.parseLong(inst.getName().substring(8));
			DBInstanceContainer instContainer = new DBInstanceContainer(inst
					.getPath(), instID);
			INST.put(instID, instContainer);
		}

		// TODO save an load ID in a right way
		File gidStoreNode = new File(path + "/" + PConst.nGID);
		try {
			GIDGenNode = new IdGeneratorImpl(gidStoreNode.getAbsolutePath(),
					100);
		} catch (Exception e) {
			gidStoreNode.delete();
			IdGeneratorImpl.createGenerator(gidStoreNode.getAbsolutePath());
			// TODO rebuild generator
			GIDGenNode = new IdGeneratorImpl(gidStoreNode.getAbsolutePath(),
					100);
		}

		File gidStoreRela = new File(path + "/" + PConst.rGID);
		try {
			GIDGenRela = new IdGeneratorImpl(gidStoreRela.getAbsolutePath(),
					100);
		} catch (Exception e) {
			gidStoreRela.delete();
			IdGeneratorImpl.createGenerator(gidStoreRela.getAbsolutePath());
			// TODO rebuild generator
			GIDGenRela = new IdGeneratorImpl(gidStoreRela.getAbsolutePath(),
					100);
		}

		// lookup service
		INDEX = new BDB_GIDLookupImpl(path + "/BDB");
	}

	// if no instance is found it is an empty container
	public PGraphDatabaseServiceImpl(String path, long id) {

		this.VERS = 0;

		this.SERVICE_ID = id;

		startup(path);

		// TODO put policy to a setting file
		this.placementPol = new RandomPlacement();
		for (long k : INST.keySet()) {
			placementPol.addInstance(k, getInstanceInfoFor(k));
		}
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
		if (INST.get(id).getInfo().getValue(InfoKey.NumNodes) == 0) {
			INST.remove(id);
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
			INST.put(id, instContainer);
			placementPol.addInstance(id, instContainer.getInfo());
			return true;
		}
		return false;
	}

	@Override
	public long[] getInstancesIDs() {
		long[] res = new long[INST.keySet().size()];
		int i = 0;
		for (long id : INST.keySet()) {
			res[i] = id;
			i++;
		}
		Arrays.sort(res);
		return res;
	}

	@Override
	public int getNumInstances() {
		return INST.values().size();
	}

	@Override
	public Node createNode(long gid) {
		if (PTX == null)
			throw new NotInTransactionException();
		return createNodeOn(gid, placementPol.getPosition());
	}

	@Override
	public Node createNodeOn(long instanceID) {
		if (PTX == null)
			throw new NotInTransactionException();
		// create GID
		long gid = createGID();
		return createNodeOn(gid, instanceID);
	}

	@Override
	public Node createNode() {
		if (PTX == null)
			throw new NotInTransactionException();
		return createNodeOn(placementPol.getPosition());
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
		if (!INST.containsKey(instanceID))
			throw new Error("Instance " + instanceID + " does not exist");

		// transaction support
		if (PTX == null)
			throw new NotInTransactionException();
		PTX.registerResource(instanceID);

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
			Node aimN = findGhostForNode(curN, instanceID);
			// create a new node if none has been found
			if (aimN == null) {
				aimN = INST.get(instanceID).createNode();
				aimN.setProperty(PConst.nGID, nodeGID);
			} else {
				// make aim node a none ghost
				aimN.removeProperty(PConst.IsGhost);
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
			curN.setProperty(PConst.IsGhost, aimPos);

			// update lookup and instance information
			INDEX.addNode(nodeGID, aimPos);
			INST.get(curPos[1]).logRemNode();
			INST.get(aimPos[1]).logAddNode();

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
				if (rs.hasProperty(PConst.IsGhost)) {

					// get half relation information
					long[] hRelPos = (long[]) rs.getProperty(PConst.IsGhost);
					Relationship hRel = INST.get(hRelPos[1])
							.getRelationshipById(hRelPos[2]);
					Node hRelStartNode = hRel.getStartNode();

					// Start node is a normal node
					if (!hRelStartNode.hasProperty(PConst.IsGhost)) {

						// move to the same instance so ghost construct is not
						// needed anymore
						if (instanceID == hRelPos[1]) {
							hRel.removeProperty(PConst.IsHalf);
							rs.delete();
						}

						// ghost construct need to be upgraded
						else {
							// find new start node for ghost relationship if
							// existing
							Node newGRelStartNode = null;
							newGRelStartNode = findGhostForNode(hRelStartNode,
									instanceID);
							// create ghost node if not existing
							if (newGRelStartNode == null) {
								newGRelStartNode = INST.get(instanceID)
										.createNode();
								newGRelStartNode.setProperty(PConst.nGID, sNode
										.getProperty(PConst.nGID));
								newGRelStartNode.setProperty(PConst.IsGhost,
										sNode.getProperty(PConst.IsGhost));
							}

							// create the relationships and link to half
							// relation
							Relationship newRS = newGRelStartNode
									.createRelationshipTo(aimN, rs.getType());
							newRS.setProperty(PConst.rGID, rs
									.getProperty(PConst.rGID));
							newRS.setProperty(PConst.IsGhost, rs
									.getProperty(PConst.IsGhost));

							// update half relation and its endNode
							long[] newRSPos = new long[3];
							newRSPos[0] = aimPos[0];
							newRSPos[1] = aimPos[1];
							newRSPos[2] = newRS.getId();
							hRel.setProperty(PConst.IsHalf, newRSPos);
							hRel.getEndNode().setProperty(PConst.IsGhost,
									aimPos);

							rs.delete();
						}
					}
				}
				// its a normal relation and start node has not been moved
				else if (!sNode.hasProperty(PConst.IsGhost)) {

					long[] sNodePos = new long[3];
					sNodePos[0] = curPos[0];
					sNodePos[1] = curPos[1];
					sNodePos[2] = sNode.getId();

					// find the new Start node or create it if not existing
					Node newGRelStartNode = findGhostForNode(sNode, instanceID);
					if (newGRelStartNode == null) {
						newGRelStartNode = INST.get(instanceID).createNode();
						newGRelStartNode.setProperty(PConst.nGID, sNode
								.getProperty(PConst.nGID));
						newGRelStartNode.setProperty(PConst.IsGhost, sNodePos);
					}

					// create the ghost relation and link it to half relation
					Relationship newRS = newGRelStartNode.createRelationshipTo(
							aimN, rs.getType());
					newRS.setProperty(PConst.rGID, rs.getProperty(PConst.rGID));
					long[] hRelPos = new long[3];
					hRelPos[0] = curPos[0];
					hRelPos[1] = curPos[1];
					hRelPos[2] = rs.getId();
					newRS.setProperty(PConst.IsGhost, hRelPos);

					// update half relation
					long[] newRSPos = new long[3];
					newRSPos[0] = aimPos[0];
					newRSPos[1] = aimPos[1];
					newRSPos[2] = newRS.getId();
					rs.setProperty(PConst.IsHalf, newRSPos);
				}
				// start and end node will be moved
				// relationship will be repaired by the start node
				else {
					continue;
				}

				if (!sNode.hasRelationship()
						&& sNode.hasProperty(PConst.IsGhost)) {
					nodeToDelete.add(sNode);
				}
			}

			for (Relationship rs : curN.getRelationships(Direction.OUTGOING)) {
				Node eNode = rs.getEndNode();
				long[] newRsPos;

				if (rs.hasProperty(PConst.IsHalf)) {
					// gather information on ghost relation
					long[] gRelPos = (long[]) rs.getProperty(PConst.IsHalf);
					Relationship gRel = INST.get(gRelPos[1])
							.getRelationshipById(gRelPos[2]);
					Node gRelEndNode = gRel.getEndNode();

					// both nodes have been moved to the same partition no ghost
					// construct needed anymore
					if (gRelEndNode.hasProperty(PConst.IsGhost)) {

						long[] newEndNodePos = (long[]) gRelEndNode
								.getProperty(PConst.IsGhost);
						Node newEndNode = INST.get(newEndNodePos[1])
								.getNodeById(newEndNodePos[2]);

						Relationship newRs = aimN.createRelationshipTo(
								newEndNode, rs.getType());
						newRsPos = new long[3];
						newRsPos[0] = aimPos[0];
						newRsPos[1] = aimPos[1];
						newRsPos[2] = newRs.getId();

						// copy relationship
						rs.removeProperty(PConst.IsHalf);
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
						rs.removeProperty(PConst.IsHalf);
						gRel.removeProperty(PConst.IsGhost);

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
								.getProperty(PConst.IsGhost);
						Node newENode = findGhostForNode(INST.get(eNodePos[1])
								.getNodeById(eNodePos[2]), instanceID);
						if (newENode == null) {
							newENode = INST.get(instanceID).createNode();
							newENode.setProperty(PConst.nGID, eNode
									.getProperty(PConst.nGID));
							newENode.setProperty(PConst.IsGhost, eNode
									.getProperty(PConst.IsGhost));
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
						gRel.setProperty(PConst.IsGhost, newRsPos);
						gRel.getStartNode().setProperty(PConst.IsGhost, aimPos);

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
					if (eNode.hasProperty(PConst.IsGhost)) {
						long[] newENodePos = (long[]) eNode
								.getProperty(PConst.IsGhost);
						Node newENode = INST.get(newENodePos[1]).getNodeById(
								newENodePos[2]);
						Relationship newRS = aimN.createRelationshipTo(
								newENode, rs.getType());
						rs.removeProperty(PConst.IsHalf);
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
						Node newENode = findGhostForNode(eNode, instanceID);
						if (newENode == null) {
							newENode = INST.get(instanceID).createNode();
							newENode.setProperty(PConst.nGID, eNode
									.getProperty(PConst.nGID));
							newENode.setProperty(PConst.IsGhost, eNodePos);
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
						newRS.setProperty(PConst.IsHalf, gRsPos);
						newRsPos = new long[3];
						newRsPos[0] = aimPos[0];
						newRsPos[1] = aimPos[1];
						newRsPos[2] = newRS.getId();

						// make normal relation to ghost relation
						rs.setProperty(PConst.IsGhost, newRsPos);
					}
				}

				// update lookup
				INDEX.addRela((Long) rs.getProperty(PConst.rGID), newRsPos);
				INST.get(newRsPos[1]).logAddRela();
				INST.get(curPos[1]).logAddRela();

				// mark node for deletion if not needed
				if (!eNode.hasRelationship()
						&& eNode.hasProperty(PConst.IsGhost)) {
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
		VERS++;
	}

	@Override
	public Transaction beginTx() {
		if (PTX == null) {
			PTX = new PTransaction(this);
		}
		return PTX;
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
		if (PTX == null)
			throw new NotInTransactionException();
		// create transactions if not yet existing
		for (Long containerID : INST.keySet()) {
			PTX.registerResource(containerID);
		}

		HashSet<Iterable<Node>> iterables = new HashSet<Iterable<Node>>();
		for (DBInstanceContainer db : INST.values()) {
			iterables.add(db.getAllNodes());
		}
		return new JoinedIterable(iterables, this);
	}

	@Override
	public Node getNodeById(long id) {
		if (PTX == null)
			throw new NotInTransactionException();
		long[] adr = INDEX.findNode(id);
		if (adr[0] == getServiceID() && INST.containsKey(adr[1])) {
			// create transaction if not yet existing
			PTX.registerResource(adr[1]);
			return new PNode(INST.get(adr[1]).getNodeById(adr[2]), this);
		}
		return null;
	}

	@Override
	public Node getReferenceNode() {
		if (PTX == null)
			throw new NotInTransactionException();

		// create reference node if not existing
		if (INDEX.findNode(0) == null && !INST.isEmpty()) {
			long instID = this.getInstancesIDs()[0];
			this.createNodeOn(0, instID);
		}
		return this.getNodeById(0);
	}

	@Override
	public Relationship getRelationshipById(long id) {
		if (PTX == null)
			throw new NotInTransactionException();
		long[] adr = INDEX.findRela(id);
		if (adr[0] == getServiceID() && INST.containsKey(adr[1])) {
			// create transaction if not yet existing
			PTX.registerResource(adr[1]);
			return new PRelation(INST.get(adr[1]).getRelationshipById(adr[2]),
					this);
		}
		return null;
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		if (PTX == null)
			throw new NotInTransactionException();

		// create transactions if not yet existing
		for (Long containerID : INST.keySet()) {
			PTX.registerResource(containerID);
		}

		// TODO prevent duplicates in the result
		HashSet<Iterable<RelationshipType>> iterables = new HashSet<Iterable<RelationshipType>>();
		for (DBInstanceContainer db : INST.values()) {
			iterables.add(db.getRelationshipTypes());
		}
		return null;
	}

	@Override
	public void shutdown() {
		// shutdown instances
		for (DBInstanceContainer dbInst : INST.values()) {
			dbInst.shutdown();
		}

		// shutdown lookup
		INDEX.shutdown();

		// shutdown GIDcreator
		GIDGenNode.close();
		GIDGenRela.close();
	}

	// TODO refine to make joining of the id's nicer
	private long createGID() {
		long localPart = GIDGenNode.nextId();
		String gid = SERVICE_ID + "" + localPart;
		return Long.parseLong(gid);
	}

	// utility class that joins several Iterable classes together
	class JoinedIterable implements Iterable<Node> {
		private PGraphDatabaseServiceImpl pdb;
		private Set<Iterable<Node>> joined;

		public JoinedIterable(Set<Iterable<Node>> joined,
				PGraphDatabaseServiceImpl db) {
			this.joined = joined;
		}

		@Override
		public Iterator<Node> iterator() {
			if (joined == null)
				return new JoinedIterator(null, null);
			Set<Iterator<Node>> iterSet = new HashSet<Iterator<Node>>();
			for (Iterable<Node> t : joined) {
				iterSet.add(t.iterator());
			}
			return new JoinedIterator(iterSet, pdb);
		}
	}

	// iterator on JoinedIterable.class
	class JoinedIterator implements Iterator<Node> {
		private Set<Iterator<Node>> joined;
		private PGraphDatabaseServiceImpl pdb;
		private Iterator<Node> curIter = null;
		private Node item = null;

		public JoinedIterator(Set<Iterator<Node>> joined,
				PGraphDatabaseServiceImpl db) {
			this.pdb = db;
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
					if (item.getId() == 0 || item.hasProperty(PConst.IsGhost)) {
						item = null;
					}
				}
			}

			return (item != null);
		}

		@Override
		public Node next() {
			if (hasNext()) {
				PNode res = new PNode(item, pdb);
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
		if (PTX == null)
			throw new NotInTransactionException();
		DBInstanceContainer inst = INST.get(instanceID);
		// create transaction if not existing
		PTX.registerResource(instanceID);

		// create node
		Node n = null;
		n = inst.createNode();
		n.setProperty(PConst.nGID, gid);

		// register node to lookup service
		long[] adr = { this.getServiceID(), instanceID, n.getId() };
		INDEX.addNode(gid, adr);
		INST.get(adr[1]).logAddNode();

		return new PNode(n, this);
	}

	@Override
	public boolean addInstance(long id) {
		String folder = DB_DIR.getAbsolutePath() + "/" + "instance" + id;
		DBInstanceContainer instContainer = new DBInstanceContainer(folder, id);
		// delete reference node
		Transaction tx = instContainer.beginTx();
		try {
			instContainer.getNodeById(0).delete();
			tx.success();
		} finally {
			tx.finish();
		}
		INST.put(id, instContainer);
		placementPol.addInstance(id, instContainer.getInfo());
		return true;
	}

	@Override
	public KernelEventHandler registerKernelEventHandler(KernelEventHandler arg0) {
		throw new UnsupportedOperationException(
				"Node.getGraphDatabase() not implemented");
	}

	@Override
	public <T> TransactionEventHandler<T> registerTransactionEventHandler(
			TransactionEventHandler<T> arg0) {
		throw new UnsupportedOperationException(
				"Node.getGraphDatabase() not implemented");
	}

	@Override
	public KernelEventHandler unregisterKernelEventHandler(
			KernelEventHandler arg0) {
		throw new UnsupportedOperationException(
				"Node.getGraphDatabase() not implemented");
	}

	@Override
	public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
			TransactionEventHandler<T> arg0) {
		throw new UnsupportedOperationException(
				"Node.getGraphDatabase() not implemented");
	}

	@Override
	public InstanceInfo getInstanceInfoFor(long id) {
		return INST.get(id).getInfo();
	}

	@Override
	public void resetLogging() {
		for (DBInstanceContainer cont : INST.values()) {
			cont.resetTraffic();
		}
	}

	@Override
	public void resetLoggingOn(long id) {
		INST.get(id).resetTraffic();
	}

	@Override
	public void setDBChangeLog(String file) {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}

	@Override
	public String getDBChangeLog() {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}
	

	

}
