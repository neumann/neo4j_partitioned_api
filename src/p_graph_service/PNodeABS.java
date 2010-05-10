package p_graph_service;

import org.neo4j.graphdb.Node;
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
}
