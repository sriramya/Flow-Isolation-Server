package flume;

import java.net.UnknownHostException;
import flume.Utility;
import ds.tree.RadixTreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowIsolation {
	
	 public int port = 0;//default port 9999
   
	private static final Logger logger = LoggerFactory.getLogger(FlowIsolation.class);
	public static void main(String[] args) throws UnknownHostException {
       
		    logger.info("Startting parser");
	        Parser parser = new Parser();
	        logger.info("CassandraClient started");
	        FICassandraClient client = new FICassandraClient();
	        RadixTreeImpl<String> trie = new RadixTreeImpl<String>();
	        FlowIsolation obj = new FlowIsolation();
	       
	        
	        
	        try{
	             if((parser.initializeData("input",trie,client, obj)) == 0) {
	            	 FIServer server = new FIServer(obj.port);
	            	 if (server.connect == 0){
	            		 logger.error("Unable to start FI server");
	            	 }
	            	 while(true) {
	            	        
	            	            String key_value = server.getcommand();
	            	           
	            	            if (key_value.substring(0, 3).equals("get")) {
	            	                String value = search(key_value.substring(4), trie);
	            	                 if ((server.writevalue(value)) == 0){
	            	                        logger.info("Reply sent to client");
	            	                 }
	            	                 else {
	            	                	 logger.error("Reply not sent, closing the connection....");
	            	                     server.closecurrent();
	            	                 }
	            	            }
	            	            else 
	            	               logger.error("Invalid input formate recevied from "+server.remoteclientAdd.toString());
	            	            
	            	               
	            	            
	            	            if (server.closecurrent() == -1)
	            	            	logger.error("Connection not Closed with "+server.remoteclientAdd.toString());
	            	            else 
	            	            	logger.info("Connection closing...");
	            	            	
	            	        }
	             }
	           
	        }
	        catch (Exception e) {
	            e.printStackTrace();
	            System.out.println(e);
	        }
       
    }
	
	static String  search(String ip, RadixTreeImpl<String> trie) throws UnknownHostException {
		   
		    if (Utility.ValidateIPAddress(ip)) {
		    	logger.error("Invalid IPV4 address received", ip);
		    	return null;
		    }
			String strip = Utility.getBinaryString(ip, 0);
		    int idx = 1;
		    String value = "";
		    System.out.println(ip);
		    while(strip.length() >idx-1 && trie.searchPrefix(strip.substring(0, idx), 1).size() >= 1) {
			        if (trie.contains(strip.substring(0, idx)))
			            value = trie.find(strip.substring(0, idx));// storing lpv
			        	System.out.println(value);
			        idx++;
		    }
		    
		    return value;
    }
   
}

