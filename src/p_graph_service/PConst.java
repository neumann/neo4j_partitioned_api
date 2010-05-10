package p_graph_service;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import p_graph_service.core.PGraphDatabaseServiceImpl;
public class PConst {
	
	public static final String rGID = "_rGID";
	public static final String nGID = "_nGID";
	
	public static final String IsGhost = "_IsGhost";
	public static final String IsHalf = "_IsHalf";
	// naming for the neo4j folder
	public static final String InstaceRegex = "instance\\d*";
	
	// hidden singleton storage class
	private PConst() {
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
	
}
