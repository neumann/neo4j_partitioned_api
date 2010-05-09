package p_graph_service.core;

import java.util.HashMap;

import org.neo4j.graphdb.Transaction;


public class PTransaction implements Transaction {
	private final PGraphDatabaseServiceImpl pdb;
	private HashMap<Long,Transaction> tParts;
	
	public PTransaction(PGraphDatabaseServiceImpl db) {
		this.tParts = new HashMap<Long, Transaction>();
		this.pdb = db;
	}
	
	public void registerResource(long id){
		if(!tParts.containsKey(id)){
			
//			System.out.println(tParts);
//			System.out.println(Neo4jDB.INST);
//			System.out.println(id);
			tParts.put(id, pdb.INST.get(id).beginTx());
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
		pdb.PTX = null;
	}

	@Override
	public void success() {
		for(Transaction tx : tParts.values()){
			tx.success();
		}
		
	}
}
