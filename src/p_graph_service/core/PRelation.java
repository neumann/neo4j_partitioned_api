package p_graph_service.core;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;


public class PRelation implements Relationship{
	private final long GID;
	private Relationship rela;
	private long version;
	
	//NOTE don't hand it ghost relation that resolve to half relations on other servers
	public PRelation(Relationship rel) {
		this.GID = (Long)rel.getProperty(Neo4jDB.GID);
		// it was a ghost relation so take the half relation instead
		if(rel.hasProperty(Neo4jDB.IsGhost)){
			long[] pos = (long[]) rel.getProperty(Neo4jDB.IsGhost);
			this.rela = Neo4jDB.INST.get(pos[1]).getRelationshipById(pos[2]);
		}else{
			this.rela = rel;
		}
		this.version = Neo4jDB.VERS;
	}
	
	//NOTE will go boom if relationship has been moved to an other server
	private void refresh(){
		if(version != Neo4jDB.VERS){
			long[] pos = Neo4jDB.INDEX.findRela(GID);
			rela = Neo4jDB.INST.get(pos[1]).getRelationshipById(pos[2]);
		}
	}
	
	@Override
	public void delete() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		
		// if halfRelation also delete ghostRelation
		// cannot be a ghost relation itself since wrapper always contrains the half relation
		if(rela.hasProperty(Neo4jDB.IsHalf)){
			long[] pos = (long[])rela.getProperty(Neo4jDB.IsHalf);
			Relationship gRela = Neo4jDB.INST.get(pos[1]).getRelationshipById(pos[2]);
			Node gSrtNode = gRela.getStartNode();
			gRela.delete();
			if(!gSrtNode.hasRelationship()){
				gSrtNode.delete();
			}
			// delete relation and ghostEndNode (if it has no more connections)
			Node gEndNode = rela.getEndNode();
			rela.delete();
			if(!gEndNode.hasRelationship()){
				gEndNode.delete();
			}			
		}else{
			rela.delete();
		}
		// update index
		Neo4jDB.INDEX.remRela(GID);
		long[] pos = Neo4jDB.INDEX.findRela(GID);
		Neo4jDB.INST.get(pos[1]).logRemRela();
	}

	@Override
	public Node getEndNode() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		// return a wrapped version of the end node
		return new PNode(rela.getEndNode());
	}

	@Override
	public long getId() {
		return GID;
	}
	
	@Override
	public Node[] getNodes() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Node[] res = new Node[2];
		res[0]= new PNode(rela.getStartNode());
		res[1]=getEndNode();
		return res;
	}

	@Override
	public Node getOtherNode(Node node) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Node wSrtNode = new PNode(rela.getStartNode());
		if(node.getId() == wSrtNode.getId()){
			return getEndNode();
		}
		return wSrtNode;
	}

	@Override
	public Node getStartNode() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return new PNode(rela.getStartNode());
	}

	@Override
	public RelationshipType getType() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.getType();
	}

	@Override
	public boolean isType(RelationshipType type) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.isType(type);
	}

	@Override
	public Object getProperty(String key) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.getProperty(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.getProperty(key, defaultValue);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.getPropertyKeys();
	}

	@Override
	@Deprecated
	public Iterable<Object> getPropertyValues() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.getPropertyValues();
	}

	@Override
	public boolean hasProperty(String key) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.hasProperty(key);
	}

	@Override
	public Object removeProperty(String key) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		return rela.removeProperty(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		rela.setProperty(key, value);
	}
}
