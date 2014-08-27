package OPM2PROV.OPM2PROV;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

import com.connexience.server.model.logging.provenance.model.nodes.DataVersion;
import com.connexience.server.model.logging.provenance.model.nodes.Library;
import com.connexience.server.model.logging.provenance.model.nodes.ServiceRun;
import com.connexience.server.model.logging.provenance.model.nodes.ServiceVersion;
import com.connexience.server.model.logging.provenance.model.nodes.TransientData;
import com.connexience.server.model.logging.provenance.model.nodes.User;
import com.connexience.server.model.logging.provenance.model.nodes.WorkflowRun;
import com.connexience.server.model.logging.provenance.model.nodes.WorkflowVersion;
import com.connexience.server.model.logging.provenance.model.relationships.ProvenanceRelationshipTypes;

public class CreateDB {
	private static  String DB_PATH;
	private static GraphDatabaseService graphDb;
	private Transaction tx;
	public static Map<Long, Node> listNode = new HashMap<Long, Node>();


	private static enum RelTypes implements RelationshipType {
		PLAN,WAS_ASSOCIATED_WITH,WAS_STARTED_BY,WAS_DERIVED_FROM, WAS_INFORMED_BY, USED, WAS_GENERATED_BY, HAD, WITH_PLAN, WAS_ATTRIBUTED_TO;
	}

	CreateDB(String Path){
		DB_PATH=Path;
	}
	
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("java -jar param1 param2");
			System.out.println("param1 database address,param2 new database address");
			System.out.println("For Example:graph.db newgraph.db"); 
		}else{
		CreateDB hello = new CreateDB(args[1]);
		// System.out.println(graphDb.index().nodeIndexNames());

		hello.createDb(args[0]);
		// hello.removeData();
		hello.shutDown();
		}
	}

	@SuppressWarnings("deprecation")
	/**
	 * this method used to create the nodes
	 * @param path
	 */
	private void createDb(String path) {
		clearDb();
		// START SNIPPET: startDb
		graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(DB_PATH)
				.newGraphDatabase();

		registerShutdownHook(graphDb);

		// END SNIPPET: startDb
		tx = graphDb.beginTx();
		try {
			App app = new App(path);
			app.setUp();
			Iterable<Node> nodes = app.printAllDataVersions();

			for (Node node : nodes) {
				addNode(node);
			}
			for (Node node : nodes) {
				addRelationship(node);
			}
			app.down();
			tx.success();
		} finally {
			tx.finish();
		}
		// END SNIPPET: transaction

		// START SNIPPET: transaction
	}

	/**
	 * used for clear the database
	 */
	private void clearDb() {
		try {
			FileUtils.deleteRecursively(new File(DB_PATH));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		// START SNIPPET: shutdownServer
		graphDb.shutdown();
		// END SNIPPET: shutdownServer
	}

	// START SNIPPET: shutdownHook
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	// END SNIPPET: shutdownHook

	/**
	 * add a node which from the original database
	 * @param node
	 */
	public static void addNode(Node node) {
		if (node.hasProperty("TYPE")) {
			String str = (String) node.getProperty("TYPE");
			switch (str) {
			case "Service Version":
				ServiceVersion sv = new ServiceVersion(node);
				listNode.put(node.getId(), createServiceNode(sv));
				break;
			case "Library":
				Library li = new Library(node);
				listNode.put(node.getId(), createLibraryNode(li));
				break;
			case "Workflow Run":
				WorkflowRun wr = new WorkflowRun(node);
				listNode.put(node.getId(), createWorkflowRunNode(wr));
				break;
			case "Transient Data":
				TransientData td = new TransientData(node);
				listNode.put(node.getId(), createTransientDataNode(td));
				break;
			case "DataVersion":
				DataVersion dv = new DataVersion(node);
				listNode.put(node.getId(), createDataVersionNode(dv));
				break;
			case "Service Run":
				ServiceRun sr = new ServiceRun(node);
				listNode.put(node.getId(), createServiceRunNode(sr));
				break;
			case "Workflow Version":
				WorkflowVersion wv = new WorkflowVersion(node);
				listNode.put(node.getId(), createWorkflowVersionNode(wv));
				break;
			case "User":
				User u = new User(node);
				listNode.put(node.getId(), createUserNode(u));
				break;
			default:
				System.out.println("unknow type of node");
				break;
			}
		}

	}

	private static Node createServiceNode(ServiceVersion serviceversion) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Entity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier", serviceversion.getEscId());
		node23.setProperty("prov:type", "ServiceVersion");		
		node23.setProperty("escid", serviceversion.getEscId());
		//nodeIndex.add(node23, "escid", serviceversion.getEscId());
		node23.setProperty("name", serviceversion.getName());
		node23.setProperty("versionId", serviceversion.getVersionId());
		//node23.setProperty("versionnumber", serviceversion.getVersionNum());
		return node23;
	}

	private static Node createLibraryNode(Library library) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Entity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier",library.getEscId()+"-"+library.getVersionNum());
		node23.setProperty("prov:type", "Library");
		node23.setProperty("escid", library.getEscId());
		node23.setProperty("name", library.getName());
		node23.setProperty("versionId", library.getVersionId());
		node23.setProperty("versionnumber", library.getVersionNum());
		return node23;

	}

	private static Node createWorkflowRunNode(WorkflowRun workflowrun) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Activity");
		node23.addLabel(myLabel);

		node23.setProperty("__identifier",workflowrun.getInvocationId());
		node23.setProperty("prov:type", "WorkflowRun");
	    if (workflowrun.getName() != null) {
					node23.setProperty("name", workflowrun.getName());
				}
		
		// node23.setProperty("escid", workflowrun.getEscId());

		
		node23.setProperty("invocationId", workflowrun.getInvocationId());
		return node23;
	}

	private static Node createTransientDataNode(TransientData transientdata) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Entity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier", transientdata.getId()+"-"+transientdata.getHashValue());

		node23.setProperty("prov:type", "TransientData");
		node23.setProperty("name", transientdata.getName());
		node23.setProperty("id", transientdata.getId());
		node23.setProperty("DataSize", transientdata.getDataSize());
		node23.setProperty("DataType", transientdata.getDataType());
		node23.setProperty("HashValue", transientdata.getHashValue());
		return node23;
	}

	private static Node createDataVersionNode(DataVersion dataversion) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Entity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier",dataversion.getEscId()+"-"+dataversion.getDocumentId());
		node23.setProperty("prov:type", "DateVersion");
		node23.setProperty("escid", dataversion.getEscId());
		node23.setProperty("name", dataversion.getName());
		node23.setProperty("versionId", dataversion.getVersionId());
		node23.setProperty("versionnumber", dataversion.getVersionNum());
		node23.setProperty("documetId", dataversion.getDocumentId());
		return node23;
	}

	private static Node createServiceRunNode(ServiceRun servicerun) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Activity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier",servicerun.getInvocationId()+"-"+servicerun.getBlockUuid());
		node23.setProperty("prov:type", "ServiceRun");
		// node23.setProperty("escid", servicerun.getEscId());
		node23.setProperty("name", servicerun.getName());
		node23.setProperty("invocationId", servicerun.getInvocationId());
		node23.setProperty("blockUUID", servicerun.getBlockUuid());
		try {
			node23.setProperty("__startTime", servicerun.getStartTime());
		} catch (org.neo4j.graphdb.NotFoundException e) {
			System.out.println("strat time is null");
		}

		try {
			node23.setProperty("__endTime", servicerun.getEndTime());
		} catch (org.neo4j.graphdb.NotFoundException e) {

		}
		return node23;
		// System.out.println( servicerun.getPropertiesData());
		// node23.setProperty("propertiesData",servicerun.getPropertiesData());
	}

	private static Node createWorkflowVersionNode(WorkflowVersion workflowrun) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Entity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier",workflowrun.getVersionId()+"-"+workflowrun.getVersionNum());
		node23.setProperty("prov:type", "WorkflowVersion");
		node23.setProperty("escid", workflowrun.getEscId());
		node23.setProperty("name", workflowrun.getName());
		node23.setProperty("versionId", workflowrun.getVersionId());
		node23.setProperty("versionnumber", workflowrun.getVersionNum());
		return node23;
	}

	private static Node createUserNode(User user) {
		Node node23 = graphDb.createNode();
		Label myLabel = DynamicLabel.label("Entity");
		node23.addLabel(myLabel);
		node23.setProperty("__identifier",user.getName()+"-"+user.getEscId());
		node23.setProperty("prov:type", "User");
		node23.setProperty("escid", user.getEscId());
		node23.setProperty("name", user.getName());
		return node23;
	}

	/**
	 * this metho used to create relationship for each nodes
	 * @param node
	 */
	private static void addRelationship(Node node) {
		if (node.hasRelationship(Direction.OUTGOING)) {
			Iterable<Relationship> relationships = node
					.getRelationships(Direction.OUTGOING);
			for (Relationship relationship : relationships) {

				if (relationship.getType().name()
						.equals(ProvenanceRelationshipTypes.INSTANCE_OF.name())
						|| relationship
								.getType()
								.name()
								.equals(ProvenanceRelationshipTypes.RUN_OF
										.name())) {
					Label myLabel = DynamicLabel
							.label("__Hyperedge");
					Node assocition = graphDb.createNode(myLabel);
					assocition.addLabel(myLabel);
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					Relationship rs = newnode.createRelationshipTo(assocition,
							RelTypes.WAS_ASSOCIATED_WITH);
					Relationship rs2 = assocition.createRelationshipTo(endnode,
							RelTypes.PLAN);
				} else if (relationship
								.getType()
								.name()
								.equals(ProvenanceRelationshipTypes.USED.name())) {
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					System.out.println(newnode + "  " + endnode + " " + node);
					Relationship rs = newnode.createRelationshipTo(endnode,
							RelTypes.USED);
					rs.setProperty("role", relationship.getProperty("role"));
					rs.setProperty("account",
							relationship.getProperty("account"));
				} else if (relationship.getType().name()
						.equals(ProvenanceRelationshipTypes.REQUIRED.name())) {
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					Relationship rs = newnode.createRelationshipTo(endnode,
							RelTypes.USED);
				} else if (relationship.getType().name()
						.equals(ProvenanceRelationshipTypes.CONTAINED.name())) {
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					Relationship rs = newnode.createRelationshipTo(endnode,
							RelTypes.WAS_INFORMED_BY);
				} else if (relationship
								.getType()
								.name()
								.equals(ProvenanceRelationshipTypes.VERSION_OF
										.name())) {
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					Relationship rs = newnode.createRelationshipTo(endnode,
							RelTypes.WAS_INFORMED_BY);
				} else if(relationship.getType().name()
						.equals(ProvenanceRelationshipTypes.INVOKED_BY.name())
						) {
					
					Label myLabel = DynamicLabel
							.label("__Hyperedge");
					Node assocition = graphDb.createNode(myLabel);
					assocition.addLabel(myLabel);
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					Relationship rs = newnode.createRelationshipTo(assocition,
							RelTypes.WAS_STARTED_BY);
					Relationship rs2 = assocition.createRelationshipTo(endnode,
							RelTypes.WAS_STARTED_BY);
				}
				else if(relationship
						.getType()
						.name()
						.equals(ProvenanceRelationshipTypes.WAS_GENERATED_BY
								.name())){
					Node newnode = getNode(node);
					Node endnode = getNode(relationship.getEndNode());
					Relationship rs = newnode.createRelationshipTo(endnode,
							RelTypes.WAS_GENERATED_BY);
					
				} else
					System.out.println("mistake");
				}
				// here

			}
		}

	/**
	 *  given a old node 
	 * @param node
	 * @return a node from the new database.
	 */
	private static Node getNode(Node node) {
		return listNode.get(node.getId());

	}
}
