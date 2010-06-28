package p_graph_service.sim;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.PlacementPolicy;
import p_graph_service.core.InstanceInfo;
import p_graph_service.core.InstanceInfo.InfoKey;
import p_graph_service.policy.RandomPlacement;

public class PGraphDatabaseServiceSIM implements PGraphDatabaseService {
	public final static String col = "_color";

	File changeLogFile;
	PrintStream log;
	final String logDelim = ";";
	private long SERVICE_ID;
	private GraphDatabaseService db;
	private PlacementPolicy placementPol;
	long VERS;
	// neo4j instances
	public HashMap<Byte, InstanceInfo> INST;
	// db folder
	protected File DB_DIR;

	public PGraphDatabaseServiceSIM(String folder, long instID, String file) {
		initialize(folder, instID, file, new RandomPlacement());
	}

	public PGraphDatabaseServiceSIM(String folder, long instID) {
		initialize(folder, instID, "changeOpLog.txt", new RandomPlacement());
	}

	public PGraphDatabaseServiceSIM(String folder, long instID, String file,
			PlacementPolicy pol) {
		initialize(folder, instID, file, pol);
	}

	public PGraphDatabaseServiceSIM(String folder, long instID,
			PlacementPolicy pol) {
		initialize(folder, instID, "changeOpLog.txt", pol);
	}

	@SuppressWarnings("unchecked")
	private void initialize(String folder, long instID, String log,
			PlacementPolicy pol) {

		this.db = new EmbeddedGraphDatabase(folder);
		this.SERVICE_ID = instID;

		this.VERS = 0;
		this.INST = new HashMap<Byte, InstanceInfo>();
		this.DB_DIR = new File(folder);
		this.placementPol = pol;

		// load stored meta information
		try {
			InputStream fips = new FileInputStream(new File(DB_DIR
					.getAbsolutePath()
					+ "/info"));
			ObjectInputStream oips = new ObjectInputStream(fips);
			this.INST = (HashMap<Byte, InstanceInfo>) oips.readObject();
			oips.close();
			fips.close();
		} catch (Exception e) {
			// cant read metha info -> create it
			Transaction tx = db.beginTx();
			try {

				for (Node n : db.getAllNodes()) {
					Byte pos = (Byte) n.getProperty(col, null);
					if (pos == null) {
						if (INST.size() == 0) {
							addInstance();
						}
						pos = new Byte((byte) placementPol.getPosition());
						n.setProperty(col, pos);
						System.out.println(n.getId()
								+ " didnt had a color and was put to " + pos);
					}
					InstanceInfo inf = INST.get(pos);
					if (inf == null) {
						inf = new InstanceInfo();
					}

					for (@SuppressWarnings("unused")
					Relationship rel : n.getRelationships(Direction.OUTGOING)) {
						inf.log(InfoKey.rs_create);
					}

					inf.log(InfoKey.n_create);
					INST.put(pos, inf);
				}
				tx.success();
			} finally {
				tx.finish();
			}

			for (InstanceInfo inf : INST.values()) {
				inf.resetTraffic();
			}
		}

		for (byte b : INST.keySet()) {
			placementPol.addInstance(b, INST.get(b));
		}

		changeLogFile = new File(folder + "/" + log);
		try {
			this.log = new PrintStream(changeLogFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.err.println("cant create log file");
		}
	}

	@Override
	public boolean addInstance() {
		byte high = 0;
		for (byte b : INST.keySet()) {
			if (high <= b)
				high++;
		}
		InstanceInfo inf = new InstanceInfo();
		INST.put(high, inf);
		placementPol.addInstance((long) high, inf);
		return true;
	}

	@Override
	public boolean addInstance(long id) {
		InstanceInfo inf = new InstanceInfo();
		INST.put((byte) id, inf);
		placementPol.addInstance(id, inf);
		return true;
	}

	@Override
	public Node createNode(long GID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Node createNodeOn(long instanceID) {
		byte byteID = (byte) instanceID;
		if (INST.containsKey(byteID)) {
			INST.get(byteID).log(InfoKey.Loc_Traffic);
			INST.get(byteID).log(InfoKey.n_create);
			Node n = db.createNode();
			n.setProperty(col, new Byte(byteID));

			// print to log
			log.println("Add_Node" + logDelim + n.getId() + logDelim + byteID);
			return new InfoNode(n, this);
		}
		return null;
	}

	@Override
	public Node createNodeOn(long GID, long instanceID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public InstanceInfo getInstanceInfoFor(long id) {
		return INST.get((byte) id);
	}

	@Override
	public long[] getInstancesIDs() {
		long[] res = new long[INST.keySet().size()];
		int i = 0;
		for (byte k : INST.keySet()) {
			res[i] = (long) k;
			i++;
		}
		Arrays.sort(res);
		return res;
	}

	@Override
	public int getNumInstances() {
		return INST.size();
	}

	@Override
	public PlacementPolicy getPlacementPolicy() {
		return placementPol;
	}

	@Override
	public long getServiceID() {
		return SERVICE_ID;
	}

	@Override
	public boolean migrateInstance(String path, long id) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void moveNodes(Iterable<Node> nodes, long instanceID) {
		// aim color
		byte byteID = (byte) instanceID;
		// invalid instance id
		if (!INST.containsKey(byteID))
			return;

		// used to enforce that no node is "deleted" multiple times
		TreeSet<Relationship> delRel = new TreeSet<Relationship>();
		InstanceInfo aimInf = INST.get(byteID);
		for (Node n : nodes) {

			Node uw = ((InfoNode) n).unwrap();
			byte curPos = (Byte) uw.getProperty(col);

			// "delete" all outgoing relations
			InstanceInfo curInf = INST.get(curPos);
			for (Relationship rs : n.getRelationships(Direction.OUTGOING)) {
				Relationship uwrs = ((InfoRelationship)rs).unwrap(); 
				if (!delRel.contains(rs)) {
					delRel.add(rs);
					log.println("Del_Rel" + logDelim + uwrs.getId());
					curInf.log(InfoKey.rs_delete);
				}
			}
			// "delete" all incoming relations
			for (Relationship rs : n.getRelationships(Direction.INCOMING)) {
				Relationship uwrs = ((InfoRelationship)rs).unwrap(); 
				if (!delRel.contains(rs)) {
					delRel.add(rs);
					log.println("Del_Rel" + logDelim + uwrs.getId());
				}
			}
			// "delete" node
			log.println("Del_Node" + logDelim + uw.getId());
			curInf.log(InfoKey.n_delete);
			curInf.logMovementTo(byteID);
		}

		for (Node n : nodes) {
			// node does not need to be moved
			Node uw = ((InfoNode) n).unwrap();

			// "add" the node
			log.println("Add_Node" + logDelim + uw.getId() + logDelim + byteID);
			aimInf.log(InfoKey.n_create);
			uw.setProperty(col, new Byte(byteID));
		}

		for (Relationship rs : delRel) {
			rs = ((InfoRelationship)rs).unwrap();
			Node sNode = rs.getStartNode();
			Node eNode = rs.getEndNode();
			Byte pos = (Byte) sNode.getProperty(col);
			INST.get(pos).log(InfoKey.rs_create);
			log.println("Add_Rel" + logDelim + rs.getId() + logDelim
					+ sNode.getId() + logDelim + eNode.getId());
		}

		VERS++;
	}

	@Override
	public boolean removeInstance(long id) {
		byte byteID = (byte) id;
		if (INST.containsKey(id)) {
			if (INST.get(byteID).getValue(InfoKey.NumNodes) == 0) {
				INST.remove(byteID);
				placementPol.removeInstance(id);
				return true;
			}
		}
		return false;
	}

	@Override
	public void resetLogging() {
		for (InstanceInfo inf : INST.values()) {
			inf.resetTraffic();
		}
	}

	@Override
	public void resetLoggingOn(long id) {
		byte byteID = (byte) id;
		if (INST.containsKey(id)) {
			INST.get(byteID).resetTraffic();
		}
	}

	@Override
	public void setPlacementPolicy(PlacementPolicy pol) {
		this.placementPol = pol;
	}

	@Override
	public Transaction beginTx() {
		return db.beginTx();
	}

	@Override
	public Node createNode() {
		return createNodeOn(placementPol.getPosition());
	}

	@Override
	public boolean enableRemoteShell() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean enableRemoteShell(Map<String, Serializable> initialProperties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Node> getAllNodes() {
		return new InfoNodeIteratable(db.getAllNodes(), this);
	}

	@Override
	public Node getNodeById(long id) {
		Node n = db.getNodeById(id);
		byte pos = (Byte) n.getProperty(col);
		INST.get(pos).log(InfoKey.Loc_Traffic);
		return new InfoNode(n, this);
	}

	@Override
	public Node getReferenceNode() {
		Node n = db.getNodeById(0);
		byte pos = (Byte) n.getProperty(col);
		INST.get(pos).log(InfoKey.Loc_Traffic);
		return new InfoNode(n, this);
	}

	@Override
	public Relationship getRelationshipById(long id) {
		Relationship rs = db.getRelationshipById(id);
		byte pos = (Byte) rs.getStartNode().getProperty(col);
		INST.get(pos).log(InfoKey.Loc_Traffic);
		InfoRelationship infrel = new InfoRelationship(rs, this);
		return infrel;
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		return db.getRelationshipTypes();
	}

	@Override
	public KernelEventHandler registerKernelEventHandler(
			KernelEventHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> TransactionEventHandler<T> registerTransactionEventHandler(
			TransactionEventHandler<T> handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutdown() {
		// store meta information
		try {
			OutputStream fops = new FileOutputStream(new File(DB_DIR
					.getAbsolutePath()
					+ "/info"));

			ObjectOutputStream oops = new ObjectOutputStream(fops);
			oops.writeObject(INST);
			oops.close();
			fops.close();
		} catch (Exception e) {
			// nothing to do there
			e.printStackTrace();
		}
		db.shutdown();
		log.close();

	}

	@Override
	public KernelEventHandler unregisterKernelEventHandler(
			KernelEventHandler handler) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
			TransactionEventHandler<T> handler) {
		throw new UnsupportedOperationException();
	}

	private class InfoNodeIteratable implements Iterable<Node> {
		private Iterable<Node> iter;
		private PGraphDatabaseServiceSIM db;

		public InfoNodeIteratable(Iterable<Node> iter,
				PGraphDatabaseServiceSIM db) {
			this.iter = iter;
			this.db = db;
		}

		@Override
		public Iterator<Node> iterator() {

			return new InfoNodeIterator(iter.iterator(), db);
		}

	}

	private class InfoNodeIterator implements Iterator<Node> {
		private Iterator<Node> iter;
		private PGraphDatabaseServiceSIM db;

		public InfoNodeIterator(Iterator<Node> iter, PGraphDatabaseServiceSIM db) {
			this.iter = iter;
			this.db = db;
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public Node next() {
			// creates 1 traffic
			InfoNode n = new InfoNode(iter.next(), db);
			n.getId();
			return n;
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}

	@Override
	public void setDBChangeLog(String file) {
		try {
			log.close();
			changeLogFile = new File(file);
			log = new PrintStream(changeLogFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getDBChangeLog() {
		return changeLogFile.getAbsolutePath();
	}

}
