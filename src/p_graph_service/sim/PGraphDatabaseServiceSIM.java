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
			for (Node n : db.getAllNodes()) {
				Byte pos = (Byte) n.getProperty(col, null);
				if (pos == null) {
					throw new Error("Graph need to be colored! Can find " + col
							+ " tag on " + n.getId());
				}
				
				InstanceInfo inf = INST.get(pos);
				if(inf == null){
					inf = new InstanceInfo();
				}
				
				for (@SuppressWarnings("unused")
				Relationship rel : n.getRelationships(Direction.OUTGOING)) {
					inf.log(InfoKey.rs_create);
				}
				
				inf.log(InfoKey.n_create);
				INST.put(pos, inf);	
			}
			
			for(InstanceInfo inf : INST.values()){
				inf.resetTraffic();	
			}
		}

		for (byte b : INST.keySet()) {
			placementPol.addInstance(b, INST.get(b));
		}

		File f = new File(folder + "/" + log);
		try {
			this.log = new PrintStream(f);
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
		INST.put(high, new InstanceInfo());
		return true;
	}

	@Override
	public boolean addInstance(long id) {
		INST.put((byte) id, new InstanceInfo());
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
			n.setProperty(col, byteID);

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

		if (INST.containsKey(byteID)) {
			InstanceInfo aimInf = INST.get(byteID);

			for (Node n : nodes) {
				Node uw = ((InfoNode) n).unwrap();
				byte curPos = (Byte) uw.getProperty(col);
				if (curPos == byteID) {
					continue;
				}
				uw.setProperty(col, byteID);
				InstanceInfo curInf = INST.get(curPos);
				curInf.log(InfoKey.n_delete);
				curInf.logMovementTo(byteID);
				uw.setProperty(col, byteID);
				aimInf.log(InfoKey.n_create);
			}
			VERS++;
		}
	}

	@Override
	public boolean removeInstance(long id) {
		byte byteID = (byte) id;
		if (INST.containsKey(id)) {
			if (INST.get(byteID).getValue(InfoKey.NumNodes) == 0) {
				INST.remove(byteID);
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

}
