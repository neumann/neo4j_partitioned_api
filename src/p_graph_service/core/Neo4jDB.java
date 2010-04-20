package p_graph_service.core;

import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

import p_graph_service.GIDLookup;
public class Neo4jDB {
	
	// version number changes if nodes moved around
	protected static long VERS = 0;
	
	protected static final String rGID = "_rGID";
	protected static final String nGID = "_nGID";
	
	protected static final String IsGhost = "_IsGhost";
	protected static final String IsHalf = "_IsHalf";
	// naming for the neo4j folder
	protected static final String InstaceRegex = "instance\\d*";

	// neo4j instances
	public static HashMap<Long,DBInstanceContainer>INST;
	// berkley db lookup service
	protected static GIDLookup INDEX;
	// GID generator (basically copy of the neo4j version)
	protected static IdGenerator GIDGenRela;
	protected static IdGenerator GIDGenNode;
	// db folder
	protected static File DB_DIR;
	// transaction support
	protected static PTransaction PTX = null;
	
	// hidden singleton storage class
	private Neo4jDB() {
	}
	
	protected static void startup(String path){
		VERS = 0;
		Neo4jDB.INST = new HashMap<Long, DBInstanceContainer>();

		// load instances
		DB_DIR = new File(path);

		// create folder is not existent
		if (!DB_DIR.exists()) {
			DB_DIR.mkdirs();
		}
		
		// list DBinstances
		File[] instances = DB_DIR.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isDirectory() && file.getName().matches(
						InstaceRegex));
			}
		});

		// create found instances
		for (File inst : instances) {
			long instID = Long.parseLong(inst.getName().substring(8));
			DBInstanceContainer instContainer = new DBInstanceContainer(inst.getPath(), instID); 
			Neo4jDB.INST.put(instID, instContainer);
		}
		
		//TODO save an load ID in a right way
		File gidStoreNode = new File(path+"/"+Neo4jDB.nGID);
		try {
			GIDGenNode = new IdGeneratorImpl(gidStoreNode.getAbsolutePath(), 100);
		} catch (Exception e) {
			gidStoreNode.delete();
			IdGeneratorImpl.createGenerator(gidStoreNode.getAbsolutePath());
			//TODO rebuild generator
			GIDGenNode = new IdGeneratorImpl(gidStoreNode.getAbsolutePath(), 100);
		}
		
		File gidStoreRela = new File(path+"/"+Neo4jDB.rGID);
		try {
			GIDGenRela = new IdGeneratorImpl(gidStoreRela.getAbsolutePath(), 100);
		} catch (Exception e) {
			gidStoreRela.delete();
			IdGeneratorImpl.createGenerator(gidStoreRela.getAbsolutePath());
			//TODO rebuild generator
			GIDGenRela = new IdGeneratorImpl(gidStoreRela.getAbsolutePath(), 100);
		}
		
		// lookup service
		Neo4jDB.INDEX = new BDB_GIDLookupImpl(path+"/BDB");
	}

	// returns a implementation of the GraphDatabaseService
	// return depends on type of DB found in the folder if nothing could be found null is returned
	public static GraphDatabaseService load(String folder){
		File f = new File(folder);
		if(f.isDirectory()){
			for (String filename : f.list()) {
				// partitioned neo4j instance
				if(filename.equals("BDB")){
					return new PGraphDatabaseServiceImpl(folder,0);
				}
				// normal neo4j instance
				if(filename.equals("neostore")){
					return new EmbeddedGraphDatabase(folder);
				}
			}	
		}
				return null;
	}
	
	// finds a ghost for a node on target instance or returns null if there is none
	public static Node findGhostForNode(Node node, long instance) {
		Node res = null;

		Iterator<Relationship> it = node.getRelationships().iterator();
		while (it.hasNext() && res == null) {
			Relationship rs = it.next();
			if (rs.hasProperty(Neo4jDB.IsHalf)) {
				long[] pos = (long[]) rs.getProperty(Neo4jDB.IsHalf);
				if (pos[1] == instance) {
					res = Neo4jDB.INST.get(pos[1])
							.getRelationshipById(pos[2]).getStartNode();
				}
			} else if (rs.hasProperty(Neo4jDB.IsGhost)) {
				long[] pos = (long[]) rs.getProperty(Neo4jDB.IsGhost);
				if (pos[1] == instance) {
					res = Neo4jDB.INST.get(pos[1])
							.getRelationshipById(pos[2]).getEndNode();
				}
			}
		}
		return res;
	}
}
