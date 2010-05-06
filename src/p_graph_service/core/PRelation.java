package p_graph_service.core;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;


public class PRelation implements Relationship{
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PRelation){
			PRelation rel = (PRelation) obj;
			if(rel.getId() == GID)return true;
		}
		return false;
	}

	private final long GID;
	private long[] pos;
	private Relationship rela;
	private long version;
	
	//NOTE don't hand it ghost relation that resolve to half relations on other servers
	public PRelation(Relationship rel) {
		this.GID = (Long)rel.getProperty(Neo4jDB.rGID);
		// it was a ghost relation so take the half relation instead
		if(rel.hasProperty(Neo4jDB.IsGhost)){
			pos = (long[]) rel.getProperty(Neo4jDB.IsGhost);
			Neo4jDB.PTX.registerResource(pos[1]);
			this.rela = Neo4jDB.INST.get(pos[1]).getRelationshipById(pos[2]);
		}else{
			this.rela = rel;
			this.pos = Neo4jDB.INDEX.findRela(GID);
		}
		this.version = Neo4jDB.VERS;
	}
	
	//NOTE will go boom if relationship has been moved to an other server
	private void refresh(){
		if(version != Neo4jDB.VERS){
			pos = Neo4jDB.INDEX.findRela(GID);
			Neo4jDB.PTX.registerResource(pos[1]);
			rela = Neo4jDB.INST.get(pos[1]).getRelationshipById(pos[2]);
		}
	}
	
	@Override
	public void delete() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		// count traffic
		Neo4jDB.INST.get(pos[1]).logTraffic();
		
		// if halfRelation also delete ghostRelation
		// cannot be a ghost relation itself since wrapper always contrains the half relation
		if(rela.hasProperty(Neo4jDB.IsHalf)){
			long[] otherPos = (long[])rela.getProperty(Neo4jDB.IsHalf);
			Neo4jDB.PTX.registerResource(otherPos[1]);
			Relationship gRela = Neo4jDB.INST.get(otherPos[1]).getRelationshipById(otherPos[2]);
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
			//count traffic
			Neo4jDB.INST.get(otherPos[1]).logTraffic();
			
		}else{	
			rela.delete();
		}
		// update index
		long[] pos = Neo4jDB.INDEX.findRela(GID);
		Neo4jDB.INDEX.remRela(GID);
		Neo4jDB.INST.get(pos[1]).logRemRela();
	}

	@Override
	public Node getEndNode() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
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
		Neo4jDB.PTX.registerResource(pos[1]);
		
		Node[] res = new Node[2];
		res[0]= new PNode(rela.getStartNode());
		res[1]=getEndNode();
		return res;
	}

	@Override
	public Node getOtherNode(Node node) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		Node wSrtNode = new PNode(rela.getStartNode());
		if(node.getId() == wSrtNode.getId()){
			return getEndNode();
		}
		if(node.getId() == getEndNode().getId()){
			return wSrtNode;
		}
		throw new NotFoundException( "Node[" + node.getId()
	            + "] not connected to this relationship[" + getId() + "]" );	
	}

	@Override
	public Node getStartNode() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return new PNode(rela.getStartNode());
	}

	@Override
	public RelationshipType getType() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.getType();
	}

	@Override
	public boolean isType(RelationshipType type) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.isType(type);
	}

	@Override
	public Object getProperty(String key) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.getProperty(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.getProperty(key, defaultValue);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.getPropertyKeys();
	}

	@Override
	@Deprecated
	public Iterable<Object> getPropertyValues() {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.getPropertyValues();
	}

	@Override
	public boolean hasProperty(String key) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.hasProperty(key);
	}

	@Override
	public Object removeProperty(String key) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return rela.removeProperty(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		if(Neo4jDB.PTX == null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		rela.setProperty(key, value);
	}
	
	public long[] getPos(){
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		return pos.clone();
	}
}
