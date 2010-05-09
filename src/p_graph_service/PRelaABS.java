package p_graph_service;

import org.neo4j.graphdb.Relationship;

import p_graph_service.core.PRelation;

public abstract class PRelaABS implements Relationship, Comparable<Relationship> {

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PRelation){
			PRelation rel = (PRelation) obj;
			if(rel.getId() == getId())return true;
		}
		return false;
	}
	
	@Override
	public int compareTo(Relationship arg0) {
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

}
