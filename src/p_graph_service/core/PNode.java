package p_graph_service.core;

import java.util.Iterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

import p_graph_service.PConst;
import p_graph_service.PNodeABS;


public class PNode extends PNodeABS{
	private final PGraphDatabaseServiceImpl pdb;
	private final long GID;
	private long[] pos;
	private long version;
	private Node node;

	
	
	private Node findGhostForNode(Node node, long instance) {
		Node res = null;

		Iterator<Relationship> it = node.getRelationships().iterator();
		while (it.hasNext() && res == null) {
			Relationship rs = it.next();
			if (rs.hasProperty(PConst.IsHalf)) {
				long[] pos = (long[]) rs.getProperty(PConst.IsHalf);
				if (pos[1] == instance) {
					res = pdb.INST.get(pos[1])
							.getRelationshipById(pos[2]).getStartNode();
				}
			} else if (rs.hasProperty(PConst.IsGhost)) {
				long[] pos = (long[]) rs.getProperty(PConst.IsGhost);
				if (pos[1] == instance) {
					res = pdb.INST.get(pos[1])
							.getRelationshipById(pos[2]).getEndNode();
				}
			}
		}
		return res;
	}
	
	
	
	// constructor
	// NOTE don't give it ghost nodes that resolve to other servers or it will
	// go boom
	public PNode(Node n,  PGraphDatabaseServiceImpl db ) {
		this.pdb = db;
		this.GID = (Long) n.getProperty(PConst.nGID);
		this.pos = pdb.INDEX.findNode(GID);
		if (n.hasProperty(PConst.IsGhost)) {
			this.node = pdb.INST.get(pos[1]).getNodeById(pos[2]);
		} else {
			this.node = n;
		}
		this.version = pdb.VERS;
	}

	// refreshes reference if graph has changed
	private void refresh() {
		if (pdb.VERS != version) {
			version = pdb.VERS;
			long[] newPos = pdb.INDEX.findNode(GID);

			// throw exception when node moved to other server
			if (newPos[0] != pos[0]) {
				throw new Error("Node moved to other server");
			}

			pos = newPos;
			pdb.PTX.registerResource(pos[1]);
			node = pdb.INST.get(pos[1]).getNodeById(pos[2]);
		}
	}

	protected Node getWrappedNode() {
		refresh();
		pdb.PTX.registerResource(pos[1]);

		return node;
	}

	public long[] getPos() {
		refresh();
		pdb.PTX.registerResource(pos[1]);

		return pos.clone();
	}

	@Override
	public Relationship createRelationshipTo(Node otherNodeP,
			RelationshipType type) {

		// transaction
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);

		Node otherNodeUW = ((PNode) otherNodeP).getWrappedNode();

		// IMPORTANT! has to be atomic, it will mess up the DB when data is
		// moved while altering it
		// check own position
		long[] ownPos = pos.clone();
		// check target position
		long[] otherPos = pdb.INDEX.findNode(((PNode) otherNodeP).getId());

		// log traffic on the instances
		pdb.INST.get(ownPos[1]).logTraffic();
		pdb.INST.get(otherPos[1]).logTraffic();

		// create transactions if not already existing
		pdb.PTX.registerResource(ownPos[1]);
		pdb.PTX.registerResource(otherPos[1]);

		if (ownPos[0] != otherPos[0]) {
			throw new Error(
					"cannot create reference to nodes on different servers");
		}

		// if on same partition create link
		if (ownPos[0] == otherPos[0] && ownPos[1] == otherPos[1]) {

			// relationship with id
			long relID = pdb.GIDGenRela.nextId();
			Relationship rel = node.createRelationshipTo(otherNodeUW, type);
			rel.setProperty(PConst.rGID, relID);
			// register to lookup
			long[] relPos = new long[3];
			relPos[0] = ownPos[0];
			relPos[1] = ownPos[1];
			relPos[2] = rel.getId();
			pdb.INDEX.addRela(relID, relPos);
			pdb.INST.get(relPos[1]).logAddRela();
			// return wrapper
			return new PRelation(rel, pdb);
		}

		// if on different partition create ghostnodes and links
		if (ownPos[0] == otherPos[0] && ownPos[1] != otherPos[1]) {
			Node otherGNode = null;
			Node srtGNode = null;

			// find endGNode if existing
			otherGNode = findGhostForNode(otherNodeUW, ownPos[1]);

			// find startNode if existing
			srtGNode = findGhostForNode(node, otherPos[1]);

			// create ghost endNode if not found
			if (otherGNode == null) {
				otherGNode = pdb.INST.get(ownPos[1]).createNode();
				otherGNode.setProperty(PConst.nGID, otherNodeP.getId());
				otherGNode.setProperty(PConst.IsGhost, otherPos);
			}

			// create ghost srtNode if not found
			if (srtGNode == null) {
				srtGNode = pdb.INST.get(otherPos[1]).createNode();
				srtGNode.setProperty(PConst.nGID, GID);
				srtGNode.setProperty(PConst.IsGhost, ownPos);
			}

			// create ID
			long relID = pdb.GIDGenRela.nextId();

			// create halfRelation
			Relationship hlfRel = node.createRelationshipTo(otherGNode, type);
			hlfRel.setProperty(PConst.rGID, relID);

			// ghost relation
			Relationship gstRel = srtGNode.createRelationshipTo(otherNodeUW,
					type);
			gstRel.setProperty(PConst.rGID, relID);

			long[] hlfPos = new long[3];
			hlfPos[0] = ownPos[0];
			hlfPos[1] = ownPos[1];
			hlfPos[2] = hlfRel.getId();

			long[] gstPos = new long[3];
			gstPos[0] = otherPos[0];
			gstPos[1] = otherPos[1];
			gstPos[2] = gstRel.getId();

			// connect the relation to each other
			gstRel.setProperty(PConst.IsGhost, hlfPos);
			hlfRel.setProperty(PConst.IsHalf, gstPos);

			// register halfRelation to lookup
			pdb.INDEX.addRela(relID, hlfPos);
			pdb.INST.get(hlfPos[1]).logAddRela();

			// return wrapper
			return new PRelation(hlfRel, pdb);
		}

		return null;
	}

	@Override
	public void delete() {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);

		// NOTE: IMPORTANT! make sure all relationships are removed
		// otherwise it goes boom until transaction rollback implemented
		pdb.INST.get(pos[1]).logTraffic();

		pdb.INDEX.remNode(GID);
		pdb.INST.get(pos[1]).logRemNode();
		node.delete();
	}

	// no refresh needed since GID never changes
	@Override
	public long getId() {
		return GID;
	}

	@Override
	public Iterable<Relationship> getRelationships() {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(), pdb);
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType... types) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(types),pdb);
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(dir),pdb);
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType type,
			Direction dir) {

		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return new WrapIterable<Relationship>(node.getRelationships(type, dir), pdb);
	}

	@Override
	public Relationship getSingleRelationship(RelationshipType type,
			Direction dir) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		pdb.INST.get(pos[1]).logTraffic();
		return new PRelation(node.getSingleRelationship(type, dir), pdb);
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
	@Deprecated
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			Object... relationshipTypesAndDirections) {
		// not supported
		throw new UnsupportedOperationException(
				"LocalPNode.traverse() not supported");
	}

	@Override
	@Deprecated
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			RelationshipType relationshipType, Direction direction) {
		// not supported
		throw new UnsupportedOperationException(
				"LocalPNode.traverse() not supported");
	}

	@Override
	@Deprecated
	public Traverser traverse(Order traversalOrder,
			StopEvaluator stopEvaluator,
			ReturnableEvaluator returnableEvaluator,
			RelationshipType firstRelationshipType, Direction firstDirection,
			RelationshipType secondRelationshipType, Direction secondDirection) {
		// not supported
		throw new UnsupportedOperationException(
				"LocalPNode.traverse() not supported");
	}

	@Override
	public Object getProperty(String key) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return node.getProperty(key);
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return node.getProperty(key, defaultValue);
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return node.getPropertyKeys();
	}

	@Override
	@Deprecated
	public Iterable<Object> getPropertyValues() {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return node.getPropertyValues();
	}

	@Override
	public boolean hasProperty(String key) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return node.hasProperty(key);
	}

	@Override
	public Object removeProperty(String key) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		return node.removeProperty(key);
	}

	@Override
	public void setProperty(String key, Object value) {
		if (pdb.PTX == null)
			throw new NotInTransactionException();
		refresh();
		pdb.PTX.registerResource(pos[1]);
		node.setProperty(key, value);
	}

	// utility class replacing ghostRelations
	private class WrapIterable<T> implements Iterable<Relationship> {
		private Iterable<Relationship> iterRel;
		private PGraphDatabaseServiceImpl db;
		
		public WrapIterable(Iterable<Relationship> rel, PGraphDatabaseServiceImpl db) {
			this.iterRel = rel;
			this.db = db;
		}

		@Override
		public Iterator<Relationship> iterator() {
			return new WrapIterator<Relationship>(iterRel.iterator(), db);
		}

	}

	// iterator on WrapIterable.class
	private class WrapIterator<T> implements Iterator<Relationship> {
		private Iterator<Relationship> relIter;
		private PGraphDatabaseServiceImpl db;

		public WrapIterator(Iterator<Relationship> rel, PGraphDatabaseServiceImpl db) {
			this.relIter = rel;
			this.db = db;
		}

		@Override
		public boolean hasNext() {
			return relIter.hasNext();
		}

		@Override
		public Relationship next() {
			Relationship rel = relIter.next();
			pdb.INST.get(pos[1]).logTraffic();
			return new PRelation(rel, db);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException(
					"JoinedIterator.remove() not implemented");
		}
	}

	@Override
	public GraphDatabaseService getGraphDatabase() {
		throw new UnsupportedOperationException(
				"Node.getGraphDatabase() not implemented");
	}
}
