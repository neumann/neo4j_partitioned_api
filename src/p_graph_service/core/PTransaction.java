package p_graph_service.core;

import java.util.HashMap;

import org.neo4j.graphdb.Transaction;


public class PTransaction implements Transaction {
	private HashMap<Long,Transaction> tParts;
	
	public PTransaction() {
		this.tParts = new HashMap<Long, Transaction>();
	}
	
	public void registerResource(long id){
		if(!tParts.containsKey(id)){
			
			System.out.println(tParts);
			System.out.println(Neo4jDB.INST);
			System.out.println(id);
			tParts.put(id, Neo4jDB.INST.get(id).beginTx());
		}
	}
	
	@Override
	public void failure() {
		for(Transaction tx : tParts.values()){
			tx.failure();
		}
	}

	@Override
	public void finish() {
		for(Transaction tx : tParts.values()){
			tx.finish();
		}
		tParts.clear();
		Neo4jDB.PTX = null;
	}

	@Override
	public void success() {
		for(Transaction tx : tParts.values()){
			tx.success();
		}
		
	}
}
