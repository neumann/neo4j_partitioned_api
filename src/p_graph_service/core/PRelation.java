package p_graph_service.core;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import p_graph_service.PConst;
import p_graph_service.PRelaABS;


public class PRelation extends PRelaABS{
	private final PGraphDatabaseServiceImpl pdb;
	private final long GID;
	private long[] pos;
	private Relationship rela;
	private long version;
	
	//NOTE don't hand it ghost relation that resolve to half relations on other servers
	public PRelation(Relationship rel, PGraphDatabaseServiceImpl db) {
		this.pdb = db;
		this.GID = (Long)rel.getProperty(PConst.rGID);
		// it was a ghost relation so take the half relation instead
		if(rel.hasProperty(PConst.IsGhost)){
			pos = (long[]) rel.getProperty(PConst.IsGhost);
			pdb.PTX.registerResource(pos[1]);
			this.rela = pdb.INST.get(pos[1]).getRelationshipById(pos[2]);
		}else{
			this.rela = rel;
			this.pos = pdb.INDEX.findRela(GID);
		}
		this.version = pdb.VERS;
	}
	
	//NOTE will go boom if relationship has been moved to an other server
	private void refresh(){
		if(version != pdb.VERS){
			pos = pdb.INDEX.findRela(GID);
			pdb.PTX.registerResource(pos[1]);
			rela = pdb.INST.get(pos[1]).getRelationshipById(pos[2]);
		}
	}
	
	@Override
	public void delete() {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		// count traffic
		pdb.INST.get(pos[1]).logTraffic();
		
		// if halfRelation also delete ghostRelation
		// cannot be a ghost relation itself since wrapper always contrains the half relation
		if(rela.hasProperty(PConst.IsHalf)){
			long[] otherPos = (long[])rela.getProperty(PConst.IsHalf);
			pdb.PTX.registerResource(otherPos[1]);
			Relationship gRela = pdb.INST.get(otherPos[1]).getRelationshipById(otherPos[2]);
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
			pdb.INST.get(otherPos[1]).logTraffic();
			
		}else{	
			rela.delete();
		}
		// update index
		long[] pos = pdb.INDEX.findRela(GID);
		pdb.INDEX.remRela(GID);
		pdb.INST.get(pos[1]).logRemRela();
	}

	@Override
	public Node getEndNode() {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		// return a wrapped version of the end node
		return new PNode(rela.getEndNode(), pdb);
	}

	@Override
	public long getId() {
		return GID;
	}
	
	@Override
	public Node[] getNodes() {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		Node[] res = new Node[2];
		res[0]= new PNode(rela.getStartNode(), pdb);
		res[1]=getEndNode();
		return res;
	}

	@Override
	public Node getOtherNode(Node node) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		Node wSrtNode = new PNode(rela.getStartNode(), pdb);
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
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return new PNode(rela.getStartNode(), pdb);
	}

	@Override
	public RelationshipType getType() {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.getType();
	}

	@Override
	public boolean isType(RelationshipType type) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.isType(type);
	}

	@Override
	public Object getProperty(String key) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.getProperty(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.getProperty(key, defaultValue);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.getPropertyKeys();
	}

	@Override
	@Deprecated
	public Iterable<Object> getPropertyValues() {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.getPropertyValues();
	}

	@Override
	public boolean hasProperty(String key) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.hasProperty(key);
	}

	@Override
	public Object removeProperty(String key) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return rela.removeProperty(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		if(pdb.PTX == null) throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		rela.setProperty(key, value);
	}
	
	public long[] getPos(){
		refresh();
		pdb.PTX.registerResource(pos[1]);
		
		return pos.clone();
	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}
}
