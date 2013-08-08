package flume;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;


@SuppressWarnings("unused")
public class CassandraClient
{    
      String clusterName, server, strategy;
    
      int rf;
   
      public void createKeyspaceNoIndex(String keyspace, String cfn){
    	  
    	  Cluster myCluster = HFactory.getOrCreateCluster(this.clusterName,this.server);
          KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace(keyspace);
          ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace,cfn, ComparatorType.UTF8TYPE);
          KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace, this.strategy,this.rf, Arrays.asList(cfDef)); //Arrays.asList(cfDef, cfDefidx)
          if (keyspaceDef == null) {
                  	if (myCluster.addKeyspace(newKeyspace) == null)
                  		 System.out.println("Problem while includeing keyspace");
          }     	
          else {
	        	    myCluster.addColumnFamily(cfDef);
                    }
    	  
      }
      
      public void createKeySpace(String keyspace, String cfn) {
    	  
	          String cfndata = cfn;
	          String cfnidx = cfn+"_TimeIndex";
	          Cluster myCluster = HFactory.getOrCreateCluster(this.clusterName,this.server);
	          KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace(keyspace);
	          ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace,cfndata, ComparatorType.UTF8TYPE);
	          ColumnFamilyDefinition cfDefidx = HFactory.createColumnFamilyDefinition(keyspace,cfnidx, ComparatorType.TIMEUUIDTYPE);
	          KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace, this.strategy,this.rf, Arrays.asList(cfDef,cfDefidx)); //Arrays.asList(cfDef, cfDefidx)
	          if (keyspaceDef == null) {
	                  	if (myCluster.addKeyspace(newKeyspace) == null)
	                  		 System.out.println("Problem while includeing keyspace");
	          }     	
	          else {
		        	    myCluster.addColumnFamily(cfDef);
		        	    myCluster.addColumnFamily(cfDefidx);
	                    }
	        
      }

      public void deleteKeySpace(String keyspace) {
    	  
	          Cluster myCluster = HFactory.getOrCreateCluster(this.clusterName,this.server);
	          myCluster.dropKeyspace(keyspace);
      }
     
      public void deleteColumnFamily(String keyspace, String cnf) {
	          Cluster myCluster = HFactory.getOrCreateCluster(this.clusterName,this.server);
	          myCluster.dropColumnFamily(keyspace, cnf) ;
	          myCluster.dropColumnFamily(keyspace, cnf+"_TimeIndex") ;
      }
      
      public void deleteColumnFamilynoindex(String keyspace, String cnf) {
          Cluster myCluster = HFactory.getOrCreateCluster(this.clusterName,this.server);
          myCluster.dropColumnFamily(keyspace, cnf) ;
  }
}