package p_graph_service.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import p_graph_service.GIDLookup;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BDB_GIDLookupImpl implements GIDLookup {
	Environment environment;
	Database RelationBDB;
	Database NodeBDB;

	public BDB_GIDLookupImpl(String aim) {
		
		EnvironmentConfig environmentConfig = new EnvironmentConfig();

		// will be created if not existing -> will crash if folder not found
		environmentConfig.setAllowCreate(true);

		// no transaction yet -> might be needed later
		// not sure if needed to be set in environment and database
		environmentConfig.setTransactional(false);

		File file = new File(aim);

		if (!file.exists()) {
			file.mkdirs();
		}

		environment = new Environment(file, environmentConfig);
		DatabaseConfig databaseConfig = new DatabaseConfig();

		// will be created if not existing -> will crash if folder not found
		databaseConfig.setAllowCreate(true);

		// no transaction yet -> might be needed later
		// not sure if needed to be set in environment and database
		databaseConfig.setTransactional(false);

		// create 2 "tables" one for relations-gid one for node-gid
		RelationBDB = environment.openDatabase(null, "Relation",
				databaseConfig);
		NodeBDB = environment.openDatabase(null, "Node",
				databaseConfig);
	}

	@Override
	public void addNode(long gid, long[] pos) {
		DatabaseEntry key = longToEntry(gid);
		DatabaseEntry data = addrToEntry(pos);
		NodeBDB.put(null, key, data);
	}

	@Override
	public void addRela(long gid, long[] pos) {
		DatabaseEntry key = longToEntry(gid);
		DatabaseEntry data = addrToEntry(pos);
		RelationBDB.put(null, key, data);
	}

	@Override
	public long[] findNode(long gid) {
		DatabaseEntry key = longToEntry(gid);
		DatabaseEntry res = new DatabaseEntry();
		NodeBDB.get(null, key, res, null);
		return entryToAddr(res);
	}

	@Override
	public long[] findRela(long gid) {
		DatabaseEntry key = longToEntry(gid);
		DatabaseEntry res = new DatabaseEntry();
		RelationBDB.get(null, key, res, null);
		return entryToAddr(res);
	}

	@Override
	public void remNode(long gid) {
		DatabaseEntry key = longToEntry(gid);
		NodeBDB.removeSequence(null, key);
	}

	@Override
	public void remRela(long gid) {
		DatabaseEntry key = longToEntry(gid);
		RelationBDB.removeSequence(null, key);
	}

	@Override
	public void shutdown() {
		// ------ closing down everything
		// sync according to literature needed if no transaction used
		RelationBDB.close();
		NodeBDB.close();
		
		environment.sync();
		environment.close();
		
	}
	
	public static DatabaseEntry longToEntry(long gid){
		return new DatabaseEntry(Long.toString(gid).getBytes());
	}
	
	public static long entryToLong(DatabaseEntry entry){
		long res = -1;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(entry.getData());
			ObjectInputStream ois = new ObjectInputStream(bis);
			res = ois.readLong();
			ois.close();
			bis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public static DatabaseEntry addrToEntry(long[] addr){
		DatabaseEntry res = null;
		
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
		    ObjectOutputStream oos = new ObjectOutputStream(bos); 
			oos.writeObject(addr);
			oos.flush(); 
		    oos.close(); 
		    bos.close();
		    res = new DatabaseEntry(bos.toByteArray());  
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public static long[] entryToAddr(DatabaseEntry entry){
		long[] res = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(entry.getData());
			ObjectInputStream ois = new ObjectInputStream(bis);
			res = (long[]) ois.readObject();
			ois.close();
			bis.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return res;
	}

}
