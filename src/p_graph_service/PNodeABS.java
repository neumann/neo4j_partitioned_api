package p_graph_service;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expansion;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
public abstract class PNodeABS implements Node , Comparable<Node>{
	
	@Override
	public int compareTo(Node arg0) {
		if(arg0.getId()<getId()){
			return -1;
		}
		if(arg0.getId()>getId()){
			return 1;
		}
		return 0;
	}

	@Override
	public int hashCode() {
		return (int) getId();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PNodeABS) {
			Node pn = (Node) obj;
			if (pn.getId() == getId())
				return true;
		}
		return false;
	}
	
	@Override
	public Expansion<Relationship> expand(RelationshipType arg0) {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}



	@Override
	public Expansion<Relationship> expand(RelationshipExpander arg0) {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}



	@Override
	public Expansion<Relationship> expand(RelationshipType arg0, Direction arg1) {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}



	@Override
	public Expansion<Relationship> expandAll() {
		throw new UnsupportedOperationException(
		"Node.getGraphDatabase() not implemented");
	}
}
