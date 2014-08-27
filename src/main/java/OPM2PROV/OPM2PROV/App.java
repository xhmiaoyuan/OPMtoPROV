package OPM2PROV.OPM2PROV;


import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;


/**
 * Hello world!
 */
public class App {
    private static String DB_LOC;
    private static GraphDatabaseService graphDb;
    Transaction tx;
     App(String path){
    	 DB_LOC=path;
     }
    public void setUp() {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_LOC);
        registerShutdownHook();
        tx = graphDb.beginTx();
    }   
    @SuppressWarnings("deprecation")
	public void down(){
    	tx.success();
        tx.finish();
        
    }

    public  Iterable<org.neo4j.graphdb.Node> printAllDataVersions() {
 
    	 GlobalGraphOperations ggo= GlobalGraphOperations.at(graphDb);
    	return ggo.getAllNodes();
    }
    public void tearDown() {
        graphDb.shutdown();
    }

    private void registerShutdownHook() {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

 
}
