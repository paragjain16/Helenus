package org.ds.client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.networkConf.XmlParseUtility;
import org.ds.server.KVStoreOperation.MapType;
import org.ds.server.MostRecentOperations;
import org.ds.socket.DSocket;

/**
 * @author pjain11,mallapu2 Class responsible for accepting the below
 *         operations: insert(key,value), and lookup(key) -> value, and
 *         update(key, new_value), and delete(key)
 */
public class NodeClient {
	private String contactMachineIP;
	private int contactMachinePort;
    private static final int REPLICATION_FACTOR=3; 
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String key = null;
		String value = null;
		String consistencyLevelStr=null;
		Integer consistencyLevel = null;
		Options options = new Options();

		options.addOption("k", true, "key");
		options.addOption("v", true, "value");
		options.addOption("c", true, "consistencyLevel");
		options.addOption("l", false, "lookup");
		options.addOption("i", false, "insert");
		options.addOption("u", false, "update");
		options.addOption("d", false, "delete");
		options.addOption("q", false, "quit");
		options.addOption("s", false, "show");
		options.addOption("sr", false, "showRecent");
		options.addOption("ti", false, "test insert");
		options.addOption("tl", false, "test lookup");
		options.addOption("til", false, "test insert and lookup");

		// automatically generate the help statement
		// HelpFormatter formatter = new HelpFormatter();
		// formatter.printHelp("help", options);
		System.setProperty("logfile.name", "./machine.log");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if (cmd.hasOption("k")) {
			key = cmd.getOptionValue("k");
			Integer hashedKey = Hash.doHash(key);
			System.out.println("Entered key:" + key + " is hashed to "
					+ hashedKey);
		}

		if (cmd.hasOption("v")) {
			value = cmd.getOptionValue("v");
		}

		if (cmd.hasOption("c")) {
			consistencyLevelStr=cmd.getOptionValue("c");
			if(!consistencyLevelStr.equalsIgnoreCase("one") && !consistencyLevelStr.equalsIgnoreCase("quorum") && !consistencyLevelStr.equalsIgnoreCase("all")){
				System.out
				.println("Please specify a consistency level of ONE,QUOROUM or ALL with the request");
				System.exit(0);
			}
			if(consistencyLevelStr.equalsIgnoreCase("one")){
				consistencyLevel=1;
			}
			else if(consistencyLevelStr.equalsIgnoreCase("quorum"))
			{
				consistencyLevel=(REPLICATION_FACTOR/2)+1;
			}
			else if(consistencyLevelStr.equalsIgnoreCase("all")){
				consistencyLevel=REPLICATION_FACTOR;
			}			
		} else if (!cmd.hasOption("c")) {
			// Consistency level is mandatory for all commands except show and
			// showRecent commands
			if (!(cmd.hasOption("s")) && !(cmd.hasOption("sr"))) {
				System.out
						.println("Please specify a consistency level of ONE,QUOROUM or ALL with the request");
				System.exit(0);
			}
		}

		NodeClient client = new NodeClient();
		String contactMachineAddr = XmlParseUtility.getContactMachineAddr();
		client.contactMachineIP = contactMachineAddr.split(":")[0];
		client.contactMachinePort = Integer.parseInt(contactMachineAddr
				.split(":")[1]);

		if (cmd.hasOption("l")) {
			// Invoke the lookup method on NodeClient

			Object objValue = client.lookup(key, consistencyLevel);
			if (objValue instanceof String
					&& objValue.toString().equals("!#KEYNOTFOUND#!")) {
				System.out.println("Entered key not found in the distributed key value store");
			} else {
				String valueStr = (String) objValue;
				if (valueStr.contains(",")) {
					DSLogger.logFE("ReadNames", "read", "Retreiving titles present in line nos:"+valueStr);
					ReadNames.read(valueStr);
				} else {
					System.out.println("Object:" + objValue);
				}
			}
		}

		else if (cmd.hasOption("i")) {
			// Invoke the insert method on NodeClient
			client.insert(key, value, consistencyLevel);
		}

		else if (cmd.hasOption("u")) {
			// Invoke the update method on NodeClient
			Object objValue = client.lookup(key, consistencyLevel);
			if (objValue instanceof String
					&& objValue.toString().equals("!#KEYNOTFOUND#!")) {
				System.out.println("Update is not possible as the entered key is not found in the distributed key value store");
			} else {
				client.update(key, value, consistencyLevel);
			}
		}

		else if (cmd.hasOption("d")) {
			// Invoke the delete method on NodeClient
			Object objValue = client.lookup(key, consistencyLevel);
			if (objValue instanceof String
					&& objValue.toString().equals("!#KEYNOTFOUND#!")) {
				System.out.println("Delete is not possible as the entered key is not found in the distributed key value store");
			} else {
				client.delete(key, consistencyLevel);
			}
		}

		else if (cmd.hasOption("s")) {
			// Invoke the show method on NodeClient
			List<Map> mapList = (List<Map>) client.show(false);
			int count = 0;
			for (Map map : mapList) {
				System.out.println("*******Displaying map: "
						+ MapType.values()[count] + " ***************");
				count++;
				Map<String, Object> objMap = (Map<String, Object>) map;
				DSLogger.logAdmin("NodeClient", "main",
						"Received object in main " + objMap);
				// String id = (String) objMap.get(-1);
				// objMap.remove(-1);
				// System.out.println("At node id: " + id);
				if(objMap.size()<100){
				System.out.println("Local Hashmap of size " + objMap.size()
						+ " : " + objMap);
				}
				else{
					System.out.println("Local Hashmap of size " + objMap.size());
				}
			}
		} else if (cmd.hasOption("sr")) {
			// Invoke the update method on NodeClient
			List<MostRecentOperations<String>> recentOpsList = (List<MostRecentOperations<String>>) client
					.show(true);
			System.out
					.println("*******Displaying 10 most recent writes at this node*********");
			MostRecentOperations<String> mostRecentWrites = recentOpsList
					.get(0);
			if (mostRecentWrites != null) {
				if (mostRecentWrites.size() > 0) {
					for (int i = mostRecentWrites.size() - 1; i >= 0; i--) {
						String combinedValue = mostRecentWrites.get(i);
						System.out.println("Key: "
								+ combinedValue.split(":")[0] + "  Value:"
								+ combinedValue.split(":")[1]);
					}
				}
			}
			System.out
					.println("*******Displaying 10 most recent reads at this node*********");
			MostRecentOperations<String> mostRecentReads = recentOpsList.get(1);
			if (mostRecentReads != null) {
				if (mostRecentReads.size() > 0) {
					for (int i = mostRecentReads.size() - 1; i >= 0; i--) {
						String combinedValue = mostRecentReads.get(i);
						System.out.println("Key: "
								+ combinedValue.split(":")[0] + "  Value:"
								+ combinedValue.split(":")[1]);
					}
				}
			}

		} else if (cmd.hasOption("q")) {
			client.quit();
		} else if (cmd.hasOption("til")) {
			int[] randomKey = new int[1000];
			//String dummyValue = "";
			
			File file = new File("/tmp/mp4insert.csv");
			FileWriter fw = null;
			try {
				fw = new FileWriter(file);
				fw.append("Key Insert");
				fw.append(",");
				fw.append("Latency Insert");
				fw.append("\n");
				for (int i = 0; i < 1000; i++) {
					//randomKey[i] = new Random().nextInt(1000001);
					String keyl = "Key"+i;
					String valuel = "Value"+i;
					
					
					if(i%10==0){
						fw.append(keyl);
						fw.append(",");
						long startTime = System.currentTimeMillis();
						client.insert(keyl, valuel, consistencyLevel);
						long endTime = System.currentTimeMillis();
						fw.append(endTime - startTime + "");
						fw.append("\n");
					}else{
						client.insert(keyl, valuel, consistencyLevel);
					}
				}
				//fw.append(",");
				File file1 = new File("/tmp/mp4lookup.csv");
				FileWriter fw1 = null;
				fw1 = new FileWriter(file1);
				fw1.append("Key Lookup");
				fw1.append(",");
				fw1.append("Latency Lookup");
				fw1.append("\n");
				for (int i = 0; i < 100; i++) {
					int rnd = new Random().nextInt(1000);
					//int rndKey = randomKey[rndIndex];
					fw1.append("Key"+rnd );
					fw1.append(",");
					long startTime = System.currentTimeMillis();
					client.lookup("Key"+rnd, consistencyLevel);
					long endTime = System.currentTimeMillis();
					fw1.append(endTime - startTime + "");
					fw1.append("\n");
				}
			
				
				fw.flush();
				fw.close();
				fw1.flush();
				fw1.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		else if (cmd.hasOption("tl")) {
			int rndKey = new Random().nextInt(1000000);
			// client.insert(rndKey, "");
			System.out.println("Inserting Key " + rndKey + " hashed to "
					+ Hash.doHash(rndKey + ""));
			long startTime = System.currentTimeMillis();
			// Object objValue=client.lookup(rndKey);
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime);
			// System.out.println(objValue);
			// client.delete(rndKey);
		} else if (cmd.hasOption("ti")) {
			int rndKey = new Random().nextInt(1000000);
			System.out.println("Inserting Key " + rndKey + " hashed to "
					+ Hash.doHash(rndKey + ""));
			long startTime = System.currentTimeMillis();
			// client.insert(rndKey, "");
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime);
			// client.delete(rndKey);
		}

	}

	private void quit() {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("leave"));
		invokeCommand(objList, true);
	}

	private Object show(boolean recent) {
		List<Object> objList = new ArrayList<Object>();
		if (recent) {
			objList.add(new String("displayRecent"));
		} else {
			objList.add(new String("display"));
		}
		try {

			DSocket server = new DSocket("127.0.0.1", 3456);
			server.writeObjectList(objList);
			Object output = null;
			output = server.readObject();
			server.close();
			DSLogger.logAdmin("NodeClient", "invokeCommand", "Received object "
					+ output);
			return output;
		} catch (UnknownHostException e) {
			DSLogger.log("NodeClient", "invokeCommand", e.getMessage());
		} catch (IOException e) {
			DSLogger.log("NodeClient", "invokeCommand", e.getMessage());
		}
		// return invokeCommand(objList, true);
		return null;
	}

	public Object lookup(String key, Integer consistencyLevel) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("get"));
		objList.add(consistencyLevel);
		objList.add(key);
		return invokeCommand(objList, true);
	}

	public void insert(String key, String value, Integer consistencyLevel) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("put"));
		objList.add(consistencyLevel);
		objList.add(key);
		objList.add(value);
		invokeCommand(objList, true);
	}

	public void update(String key, String new_value, Integer consistencyLevel) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("update"));
		objList.add(consistencyLevel);
		objList.add(key);
		objList.add(new_value);
		invokeCommand(objList, true);
	}

	public void delete(String key, Integer consistencyLevel) {
		List<Object> objList = new ArrayList<Object>();
		objList.add(new String("delete"));
		objList.add(consistencyLevel);
		objList.add(key);
		invokeCommand(objList, false);
	}

	/**
	 * Invokes the appropriate command on server by establishing a socket
	 * connection to server and writes the argument list and passes back the
	 * result obtained from server back to the node client.
	 */
	private Object invokeCommand(List<Object> objList,
			boolean waitForOutputFromServer) {
		DSLogger.logAdmin("NodeClient", "invokeCommand", "Entering");
		DSLogger.logAdmin("NodeClient", "invokeCommand", objList.get(0)
				.toString());

		try {
			// System.out.println("Contacting "+contactMachineIP+" at port 4000");
			DSocket server = new DSocket(contactMachineIP, 4000);
			// System.out.println("Connection established with "+server.getSocket());
			DSLogger.logFE("NodeClient", "invokeCommand",
					"Socket connection with FE established");
			server.writeObjectList(objList);
			Object output = null;
			output = server.readObject();
			server.close();
			DSLogger.logAdmin("NodeClient", "invokeCommand", "Received object "
					+ output);
			return output;
		} catch (UnknownHostException e) {
			DSLogger.log("NodeClient", "invokeCommand", e.getMessage());
		} catch (IOException e) {
			DSLogger.log("NodeClient", "invokeCommand", e.getMessage());
		}
		return null;
	}

}
