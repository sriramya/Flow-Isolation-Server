package flume;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import java.io.BufferedReader;
//import java.math.BigInteger;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import ds.tree.RadixTreeImpl;
import flume.Utility;

@SuppressWarnings("unused")
public class Parser {

	private static final Logger logger = LoggerFactory.getLogger(FlowIsolation.class);	

private FileInputStream in;

public int initializeData(String filename, RadixTreeImpl<String> datatrie, FICassandraClient client, FlowIsolation port) throws Exception{

		String str[],str1[];
		String nwadd[], tID, ksp;
		String key;
		char ch;
		int NWc,i=0;

		try{
			in = new FileInputStream(filename);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			try {
				if((strLine = br.readLine())!=null ){
					str = strLine.split("@");
					if (str[0].equals("FIServerPort")) {
						port.port = Integer.parseInt(str[1]);
					}
				}
				if ((strLine = br.readLine())!= null) {
					     str1 = strLine.split("@");
					     if (str1[0].equals("Cassandra server")) {
							     str = str1[1].split(" ");
								 client.clusterName = str[0];
								 client.server = str[1];
								 client.rf = Integer.parseInt(str[2]);
								 client.strategy = str[3];
					     }
				}
				else {
				logger.error("Input file in NULL");
					return -1; //System.exit(0);
				}
			}
			catch(Exception e1){
		 		logger.error("Cassandra config is worng at " +e1);
		 		return -1; //System.exit(0);
			}
			
			try {   
				   
		           	Cluster cluster;
		            Keyspace keyspace;
		            cluster = getOrCreateCluster(client.clusterName, client.server);
		            client.deleteKeySpace("ListKeyspace");
	      			 client.createKeyspaceNoIndex("ListKeyspace","listksp");
				
					while((strLine = br.readLine())!= null ) { 
						    str1 = strLine.split("-");
						    if (str1[0].equals("#") && str1[1].equals("add") && str1[2].equals("Tenant")) {
								strLine = br.readLine();
					  			str = strLine.split("-");
					           	tID = str[0];
					           	ksp = str[1];
					           	NWc = Integer.parseInt(str[2]);
					           	
					           	keyspace = createKeyspace("ListKeyspace", cluster);
					            createMutator(keyspace,  new StringSerializer())
					            .addInsertion(ksp + tID, "listksp", createColumn("kspname", ksp+"-"+tID,  new StringSerializer(),
					            		new StringSerializer())).execute();
					           
					            if (cluster.describeKeyspace(ksp) != null)
					            	
					            
					            client.deleteKeySpace(ksp);
					            
					            client.createKeyspaceNoIndex(ksp,"CFNlist");
					            client.createKeyspaceNoIndex(ksp,"statistics");
					          
						      		while (NWc > 0 ) { 
						      			if ((strLine = br.readLine())!= null ) {
							      			ch = strLine.charAt(0);
							      			if (ch != '#'){
									              	str = strLine.split("\\s+");                                                       //str[0] is network address,str[1] is network mask, str[2] is columnfamilyname
									              	datatrie.insert(Utility.getBinaryString(str[0], Integer.parseInt(str[1])), ksp+" "+str[2]);   // insert data into Trie
									              	client.createKeySpace(ksp,str[2]);      
										            NWc--;
										         
										            keyspace = createKeyspace(ksp, cluster);
										            createMutator(keyspace,  new StringSerializer())
										            .addInsertion(str[0]+str[2], "CFNlist", createColumn("cfn",str[2],  new StringSerializer(),
										             new StringSerializer())).execute();
							      			}
							      			
							      			
						      		    }
						      			
						      			
							      			else {
							      				logger.error("Worng Input formate");
							      				return -1; //System.exit(0);
							      			}	
							            }
					   }
					   else if (str1[0].equals("#") && str1[1].equals("del") && str1[2].equals("Tenant")) {
						    strLine = br.readLine();
				  			str = strLine.split("\\s+");
				           	tID = str[0];
				           	ksp = str[1];
				           	NWc = Integer.parseInt(str[2]);
				          
					      		while (NWc > 0 ) { 
					      			if ((strLine = br.readLine())!= null ) {
						      			ch = strLine.charAt(0);
						      			if (ch != '#'){
								              	str = strLine.split("\\s+");                                                 
								              datatrie.delete(Utility.getBinaryString(str[0], Integer.parseInt(str[1])));
								              	
									            NWc--;
						      			}
					      			}
					      		}
					      		client.deleteKeySpace(ksp) ;
					   }
					   else if (str1[0].equals("#") && str1[1].equals("del") && str1[2].equals("Network")) {
				      	           if ((strLine = br.readLine())!= null ) {
						      			 str = strLine.split(" ");
						      			 datatrie.delete(Utility.getBinaryString(str[0], Integer.parseInt(str[1])));
						      			 client.deleteColumnFamily(str[3], str[2]);
					      			}
				      }  
					    
					   else {
						logger.error("Invalid network addreses");
						return -1; //System.exit(0);
					   }
					}
				}
	catch(Exception e2){
 		logger.error(e2.toString());
 		return -1; //
	}
	
	}
	catch(Exception e3){
 		logger.error(e3.toString());
 		return -1; //
	}
	return 0;	
	}

  
	
}	
