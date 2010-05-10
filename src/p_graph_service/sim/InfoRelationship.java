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
	
	public void logReadRelationship(byte curPos){
		if(curPos!=pos){
			inf.logInterComunication(curPos);
			inf.log(InfoKey.Loc_Traffic);
		}	
	}
	
	public void logWriteRelationship(byte curPos){
		if(curPos!=pos){
			inf.logInterComunication(curPos);
			db.getInstanceInfoFor(curPos).log(InfoKey.Loc_Traffic);
		}
	}
	
	
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
		byte otherPos = (Byte) rs.getEndNode().getProperty(PGraphDatabaseServiceSIM.col);
		logWriteRelationship(otherPos);
		rs.delete();
	}

	@Override
	public Node getEndNode() {
		refresh();
		log(InfoKey.Loc_Traffic);
		Node end = rs.getEndNode();
		logWriteRelationship((Byte) end.getProperty(PGraphDatabaseServiceSIM.col));
		return new InfoNode(end, db);
	}

	@Override
	public long getId() {
		refresh();
		log(InfoKey.Loc_Traffic);
		return rs.getId();
	}

	@Override
	public Node[] getNodes() {
		refresh();
		Node[] n = new Node[2];
		n[0] = getStartNode();
		n[1] = getEndNode();
		return n;
	}

	@Override
	public Node getOtherNode(Node arg0) {
		refresh();
		if(arg0 == rs.getStartNode()){
			return getEndNode();
		}
		if(arg0 == rs.getEndNode()){
			return getStartNode();
		}
		return null;
	}

	@Override
	public Node getStartNode() {
		refresh();
		log(InfoKey.Loc_Traffic);
		return new InfoNode(rs.getStartNode(),db);
	}

	@Override
	public RelationshipType getType() {
		refresh();
		log(InfoKey.Loc_Traffic);
		return rs.getType();
	}

	@Override
	public boolean isType(RelationshipType arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return rs.isType(arg0);
	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		return db;
	}

	@Override
	public Object getProperty(String arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return rs.getProperty(arg0);
	}

	@Override
	public Object getProperty(String arg0, Object arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
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
		log(InfoKey.Loc_Traffic);
		return rs.hasProperty(arg0);
	}

	@Override
	public Object removeProperty(String arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return rs.removeProperty(arg0);
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
		rs.setProperty(arg0, arg1);
	}
}
