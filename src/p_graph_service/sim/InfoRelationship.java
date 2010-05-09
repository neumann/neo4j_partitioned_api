package p_graph_service.sim;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import p_graph_service.PRelaABS;
import p_graph_service.core.InstanceInfo;
import p_graph_service.core.InstanceInfo.InfoKey;

public class InfoRelationship extends PRelaABS{
	private Relationship rs;
	private final PGraphDatabaseServiceSIM db;
	private InstanceInfo inf;
	private long VERS;
	private byte pos;
	
	private byte findSelf(){
		return (Byte)(rs.getStartNode().getProperty(PGraphDatabaseServiceSIM.col));
	}
	
	private void refresh(){
		if(VERS != db.VERS){
			pos = findSelf();
			inf = db.getInstanceInfoFor(pos);
			VERS = db.VERS;
		}
	}
	
	protected Relationship unwrap() {
		return rs;
	}

	private void log(InfoKey key) {
		inf.log(key);
	}

	public InfoRelationship(Relationship rs, PGraphDatabaseServiceSIM db) {
		if (rs instanceof InfoRelationship)
			throw new Error("dont hand and inforel to an inforel constructor");
		this.rs = rs;
		this.db = db;
		this.VERS = db.VERS;
		this.pos = findSelf();
		this.inf = db.getInstanceInfoFor(pos);
	}

	@Override
	public void delete() {
		refresh();
		log(InfoKey.rs_delete);
		rs.delete();
	}

	@Override
	public Node getEndNode() {
		refresh();
		log(InfoKey.Traffic);
		inf.logHop(pos, rs);
		return new InfoNode(rs.getEndNode(), db);
	}

	@Override
	public long getId() {
		refresh();
		log(InfoKey.Traffic);
		return rs.getId();
	}

	@Override
	public Node[] getNodes() {
		refresh();
		log(InfoKey.Traffic);
		Node[] n = rs.getNodes();
		InfoNode[] res = new InfoNode[n.length];
		for (int i = 0; i < n.length; i++) {
			res[i] = new InfoNode(n[i], db);
		}
		return res;
	}

	@Override
	public Node getOtherNode(Node arg0) {
		refresh();
		inf.logHop(pos, rs);
		log(InfoKey.Traffic);
		return new InfoNode(rs.getOtherNode(((InfoNode) arg0).unwrap()), db);
	}

	@Override
	public Node getStartNode() {
		refresh();
		inf.logHop(pos, rs);
		log(InfoKey.Traffic);
		return new InfoNode(rs.getStartNode(),db);
	}

	@Override
	public RelationshipType getType() {
		refresh();
		log(InfoKey.Traffic);
		return rs.getType();
	}

	@Override
	public boolean isType(RelationshipType arg0) {
		refresh();
		log(InfoKey.Traffic);
		return rs.isType(arg0);
	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		return db;
	}

	@Override
	public Object getProperty(String arg0) {
		refresh();
		log(InfoKey.Traffic);
		return rs.getProperty(arg0);
	}

	@Override
	public Object getProperty(String arg0, Object arg1) {
		refresh();
		log(InfoKey.Traffic);
		return rs.getProperty(arg0, arg1);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		return rs.getPropertyKeys();
	}

	@Override
	public Iterable<Object> getPropertyValues() {
		return getPropertyValues();
	}

	@Override
	public boolean hasProperty(String arg0) {
		refresh();
		log(InfoKey.Traffic);
		return rs.hasProperty(arg0);
	}

	@Override
	public Object removeProperty(String arg0) {
		refresh();
		log(InfoKey.Traffic);
		return rs.removeProperty(arg0);
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		refresh();
		log(InfoKey.Traffic);
		rs.setProperty(arg0, arg1);
	}
}
