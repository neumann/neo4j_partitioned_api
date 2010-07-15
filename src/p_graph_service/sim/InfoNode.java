package p_graph_service.sim;


import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

import p_graph_service.InstanceInfo;
import p_graph_service.PNodeABS;
import p_graph_service.InstanceInfo.InfoKey;

@SuppressWarnings("deprecation")
public class InfoNode extends PNodeABS{
	private final PGraphDatabaseServiceSIM db;
	private Node n;
	private long VERS;
	private byte pos;
	private InstanceInfo inf;
	
	
	private byte findSelf(){
		Byte res = null;
		res = (Byte)(n.getProperty(PGraphDatabaseServiceSIM.col, null));
		if(res == null)throw new Error("Node "+ n.getId() + "not colored");
		return res;
	}
	
	private void refresh(){
		if(VERS != db.VERS){
			pos = findSelf();
			inf = db.getInstanceInfoFor(pos);
			VERS = db.VERS;
		}
	}
	
	protected Node unwrap(){
		return n;
	}

	private void log(InfoKey key) {
		inf.log(key);
	}

	public InfoNode(Node n, PGraphDatabaseServiceSIM db) {
		if(n instanceof InfoNode)throw new Error("dont hand and infoNode to an infoNode constructor");
		this.n = n;
		this.db = db;
		this.VERS = db.VERS;
		this.pos = findSelf();
		this.inf = db.getInstanceInfoFor(pos);
	}

	@Override
	public Relationship createRelationshipTo(Node arg0, RelationshipType arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
		log(InfoKey.rs_create);
		Relationship newRel = n.createRelationshipTo(((InfoNode)arg0).unwrap(), arg1);
		InfoRelationship infRel = new InfoRelationship(newRel,db);
		infRel.logWriteRelationship((Byte) n.getProperty(PGraphDatabaseServiceSIM.col));
		db.log.println("Add_Rel" + db.logDelim+infRel.getId()+db.logDelim+newRel.getStartNode().getId()+db.logDelim+newRel.getEndNode().getId());
		return infRel;
	}

	@Override
	public void delete() {
		refresh();
		log(InfoKey.Loc_Traffic);
		log(InfoKey.n_delete);
		db.log.println("Del_Node"+db.logDelim+n.getId());
		n.delete();
	}

	@Override
	public long getId() {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.getId();
	}

	@Override
	public Iterable<Relationship> getRelationships() {
		refresh();
		return new InfoRelaIteratable(n.getRelationships());
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType... arg0) {
		refresh();
		return new InfoRelaIteratable(n.getRelationships(arg0));
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction arg0) {
		refresh();
		return new InfoRelaIteratable(n.getRelationships(arg0));
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType arg0,
			Direction arg1) {
		refresh();
		return new InfoRelaIteratable(n.getRelationships(arg0, arg1));
	}

	@Override
	public Relationship getSingleRelationship(RelationshipType arg0,
			Direction arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
		Relationship rs = n.getSingleRelationship(arg0, arg1);
		if(rs == null) return null;
		InfoRelationship infRel = new InfoRelationship(rs,db);
		infRel.logReadRelationship(pos);
		return new InfoRelationship(n.getSingleRelationship(arg0, arg1),db);
	}

	@Override
	public boolean hasRelationship() {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.hasRelationship();
	}

	@Override
	public boolean hasRelationship(RelationshipType... arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.hasRelationship(arg0);
	}

	@Override
	public boolean hasRelationship(Direction arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.hasRelationship(arg0);
	}

	@Override
	public boolean hasRelationship(RelationshipType arg0, Direction arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.hasRelationship(arg0, arg1);
	}

	@Override
	public Traverser traverse(Order arg0, StopEvaluator arg1,
			ReturnableEvaluator arg2, Object... arg3) {
		return n.traverse(arg0, arg1, arg2, arg3);
	}

	@Override
	public Traverser traverse(Order arg0, StopEvaluator arg1,
			ReturnableEvaluator arg2, RelationshipType arg3, Direction arg4) {
		return n.traverse(arg0, arg1, arg2, arg3, arg4);
	}

	@Override
	public Traverser traverse(Order arg0, StopEvaluator arg1,
			ReturnableEvaluator arg2, RelationshipType arg3, Direction arg4,
			RelationshipType arg5, Direction arg6) {
		return n.traverse(arg0, arg1, arg2, arg3, arg5, arg6);
	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		return db;
	}

	@Override
	public Object getProperty(String arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.getProperty(arg0);
	}

	@Override
	public Object getProperty(String arg0, Object arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.getProperty(arg0, arg1);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		return n.getPropertyKeys();
	}

	@Override
	public Iterable<Object> getPropertyValues() {
		return n.getPropertyValues();
	}

	@Override
	public boolean hasProperty(String arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.hasProperty(arg0);
	}

	@Override
	public Object removeProperty(String arg0) {
		refresh();
		log(InfoKey.Loc_Traffic);
		return n.removeProperty(arg0);
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		refresh();
		log(InfoKey.Loc_Traffic);
		n.setProperty(arg0, arg1);
	}

	private class InfoRelaIteratable implements Iterable<Relationship> {
		private Iterable<Relationship> iter;

		public InfoRelaIteratable(Iterable<Relationship> iter) {
			this.iter = iter;
		}

		@Override
		public Iterator<Relationship> iterator() {

			return new InfoRelaIterator(iter.iterator());
		}

	}

	private class InfoRelaIterator implements Iterator<Relationship> {
		private Iterator<Relationship> iter;

		public InfoRelaIterator(Iterator<Relationship> iter) {
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public Relationship next() {
			InfoRelationship rel = new InfoRelationship(iter.next(), db);
			inf.log(InfoKey.Loc_Traffic);
			rel.logReadRelationship(pos);
			return rel;
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}
}
