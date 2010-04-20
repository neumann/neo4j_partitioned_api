package p_graph_service.core;

import java.util.Iterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;


import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;


public class PNode implements Node {
	private final long GID;
	private long[] pos;
	private long version;
	private Node node;
	
	// constructor
	// NOTE don't give it ghost nodes that resolve to other servers or it will go boom
	public PNode(Node n) {
		this.GID = (Long)n.getProperty(Neo4jDB.nGID);
		this.pos = Neo4jDB.INDEX.findNode(GID);
		if(n.hasProperty(Neo4jDB.IsGhost)){
			this.node = Neo4jDB.INST.get(pos[1]).getNodeById(pos[2]);
		}else {
			this.node = n;
		}
		this.version = Neo4jDB.VERS;
	}
	
	// refreshes reference if graph has changed
	private void refresh(){
		if(Neo4jDB.VERS != version){
			version = Neo4jDB.VERS;
			long[] newPos = Neo4jDB.INDEX.findNode(GID);
			
			// throw exception when node moved to other server
			if(newPos[0]!=pos[0]){
				throw new Error("Node moved to other server");
			}
			
			pos = newPos;
			node = Neo4jDB.INST.get(pos[1]).getNodeById(pos[2]);
		}
	}
		
	protected Node getWrappedNode() {
		refresh();
		return node;
	}
	
	public long[] getPos() {
		refresh();
		return pos.clone();
	}
		
	@Override
	public Relationship createRelationshipTo(Node otherNodeP,
			RelationshipType type) {
	
		// transaction
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		
		Node otherNodeUW = ((PNode)otherNodeP).getWrappedNode();
		
		//IMPORTANT! has to be atomic, it will mess up the DB when data is moved while altering it
		//check own position
		long[] ownPos = pos.clone();
		//check target position
		long[] otherPos = Neo4jDB.INDEX.findNode(((PNode)otherNodeP).getId());
	
		// log traffic on the instances
		Neo4jDB.INST.get(ownPos[1]).logTraffic();
		Neo4jDB.INST.get(otherPos[1]).logTraffic();
		
		// create transactions if not already existing
		Neo4jDB.PTX.registerResource(ownPos[1]);
		Neo4jDB.PTX.registerResource(otherPos[1]);
		
		if(ownPos[0]!= otherPos[0]){
			throw new Error("cannot create reference to nodes on different servers");
		}
		
		//if on same partition create link
		if(ownPos[0] == otherPos[0] && ownPos[1] == otherPos[1]){
			
			// relationship with id
			long relID = Neo4jDB.GIDGenRela.nextId();
			Relationship rel = node.createRelationshipTo(otherNodeUW, type);
			rel.setProperty(Neo4jDB.rGID, relID);
			// register to lookup
			long[] relPos = new long[3];
			relPos[0] = ownPos[0];
			relPos[1] = ownPos[1];
			relPos[2]=rel.getId();
			Neo4jDB.INDEX.addRela(relID, relPos);
			Neo4jDB.INST.get(relPos[1]).logAddRela();
			// return wrapper
			return new PRelation(rel);
		}
		
		//if on different partition create ghostnodes and links
		if(ownPos[0] == otherPos[0] && ownPos[1] != otherPos[1]){
			Node otherGNode = null;
			Node srtGNode = null;
			
			// find endGNode if existing
			otherGNode = Neo4jDB.findGhostForNode(otherNodeUW, ownPos[1]);
			
			// find startNode if existing
			srtGNode = Neo4jDB.findGhostForNode(node,otherPos[1]);
			
			// create ghost endNode if not found
			if(otherGNode == null){
				otherGNode = Neo4jDB.INST.get(ownPos[1]).createNode();
				otherGNode.setProperty(Neo4jDB.nGID, otherNodeP.getId());
				otherGNode.setProperty(Neo4jDB.IsGhost, otherPos);
			}
			
			// create ghost srtNode if not found
			if(srtGNode==null){
				srtGNode = Neo4jDB.INST.get(otherPos[1]).createNode();
				srtGNode.setProperty(Neo4jDB.nGID, GID);
				srtGNode.setProperty(Neo4jDB.IsGhost, ownPos);
			}
			
			// create ID
			long relID = Neo4jDB.GIDGenRela.nextId();
			
			// create halfRelation
			Relationship hlfRel = node.createRelationshipTo(otherGNode, type);
			hlfRel.setProperty(Neo4jDB.rGID, relID);
			
			// ghost relation
			Relationship gstRel = srtGNode.createRelationshipTo(otherNodeUW, type);
			gstRel.setProperty(Neo4jDB.rGID, relID);
			
			long[] hlfPos = new long[3];
			hlfPos[0] = ownPos[0];
			hlfPos[1] = ownPos[1];
			hlfPos[2]= hlfRel.getId();
			
			long[] gstPos = new long[3];
			gstPos[0] = otherPos[0];
			gstPos[1] = otherPos[1];
			gstPos[2]= gstRel.getId();
			 
			// connect the relation to each other
			gstRel.setProperty(Neo4jDB.IsGhost, hlfPos);
			hlfRel.setProperty(Neo4jDB.IsHalf, gstPos);
			
			// register halfRelation to lookup
			Neo4jDB.INDEX.addRela(relID, hlfPos);
			Neo4jDB.INST.get(hlfPos[1]).logAddRela();
			
			// return wrapper
			return new PRelation(hlfRel);
		}
		
		return null;
	}

	@Override
	public void delete() {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		
		//NOTE: IMPORTANT! make sure all relationships are removed
		//otherwise it goes boom until transaction rollback implemented	
		Neo4jDB.INST.get(pos[1]).logTraffic();
		
		Neo4jDB.INDEX.remNode(GID);
		Neo4jDB.INST.get(pos[1]).logRemNode();
		node.delete();
	}

	// no refresh needed since GID never changes
	@Override
	public long getId() {
		return GID;
	}
	
	@Override
	public Iterable<Relationship> getRelationships() {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships());
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType... types) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(types));
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(dir));
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType type,
			Direction dir) {
		
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(type, dir));
	}

	@Override
	public Relationship getSingleRelationship(RelationshipType type,
			Direction dir) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		refresh();
		Neo4jDB.PTX.registerResource(pos[1]);
		Neo4jDB.INST.get(pos[1]).logTraffic();
		return new PRelation(node.getSingleRelationship(type, dir));
	}

	@Override
	public boolean hasRelationship() {
		refresh();
		return node.hasRelationship();
	}

	@Override
	public boolean hasRelationship(RelationshipType... types) {
		refresh();
		return node.hasRelationship(types);
	}

	@Override
	public boolean hasRelationship(Direction dir) {
		refresh();
		return node.hasRelationship(dir);
	}

	@Override
	public boolean hasRelationship(RelationshipType type, Direction dir) {
		refresh();
		return hasRelationship(type, dir);
	}

	@Override
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			Object... relationshipTypesAndDirections) {
		// not supported
		throw new UnsupportedOperationException("LocalPNode.traverse() not supported");
	}

	@Override
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			RelationshipType relationshipType, Direction direction) {
		// not supported
		throw new UnsupportedOperationException("LocalPNode.traverse() not supported");
	}

	@Override
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			RelationshipType firstRelationshipType, Direction firstDirection,
			RelationshipType secondRelationshipType, Direction secondDirection) {
		// not supported
		throw new UnsupportedOperationException("LocalPNode.traverse() not supported");
	}

	@Override
	public Object getProperty(String key) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		return node.getProperty(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		return node.getProperty(key, defaultValue);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		return node.getPropertyKeys();
	}

	@Override
	@Deprecated
	public Iterable<Object> getPropertyValues() {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		return node.getPropertyValues();
	}

	@Override
	public boolean hasProperty(String key) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		return node.hasProperty(key);
	}

	@Override
	public Object removeProperty(String key) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		return node.removeProperty(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		if(Neo4jDB.PTX==null) throw new NotInTransactionException();
		Neo4jDB.PTX.registerResource(pos[1]);
		refresh();
		node.setProperty(key, value);
	}
	
	// utility class replacing ghostRelations
	private class WrapIterable<T> implements Iterable<Relationship> {
		Iterable<Relationship> iterRel;
		public WrapIterable(Iterable<Relationship> rel) {
			this.iterRel = rel;
		}

		@Override
		public Iterator<Relationship> iterator() {
			return new WrapIterator<Relationship>(iterRel.iterator());
		}

	}

	// iterator on WrapIterable.class
	private class WrapIterator<T> implements Iterator<Relationship> {
		Iterator<Relationship> relIter;
		public WrapIterator(Iterator<Relationship> rel) {
			this.relIter = rel;
		}

		@Override
		public boolean hasNext() {
			return relIter.hasNext();
		}

		@Override
		public Relationship next() {
			Relationship rel = relIter.next();
			Neo4jDB.INST.get(pos[1]).logTraffic();
			return new PRelation(rel);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"JoinedIterator.remove() not implemented");
		}
	}
}
