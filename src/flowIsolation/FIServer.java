package flume;


import java.io.*;
import java.net.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FIServer {
	private static final Logger logger = LoggerFactory.getLogger(FlowIsolation.class);
	public int connect =1;
	public SocketAddress remoteclientAdd;
	private ServerSocket server;
    private Socket client;
    private BufferedReader reader;
    private PrintWriter writer;
    public  FIServer(int portnum)
        {
            try
            {
                server = new ServerSocket(portnum);
                logger.info("FI server started....");
               
            }
            catch (Exception err)
            {
                logger.error(err.toString());
                connect=0;
            }
        }

        public String getcommand()
        {
            try
            {
                client = server.accept();//wait for new connection
                remoteclientAdd = client.getRemoteSocketAddress();
                reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                writer = new PrintWriter(client.getOutputStream(), true);

                String returnval = reader.readLine();
              //  System.out.println(returnval);
                return returnval;
            }
            catch (Exception err)
            {
            	  logger.error(err.toString());
            }
            return null;
        }
        public int writevalue( String value)
        {
            try
            {   
                writer.println(value);
                writer.close();
                reader.close();
               // System.out.println(value);
                return 0;
            }
            catch (Exception err)
            {
            	  logger.error(err.toString());
            }
            return -1;
        }
        public int closecurrent()
        {
            try
            {
                writer.close();
                reader.close();
                return 1;
            }
            catch (Exception err)
            {
            	  logger.error(err.toString());
                return -1;
            }
        }

   
}


