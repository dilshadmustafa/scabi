/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 21-Jan-2016
 * File Name : MetaServer.java
 */

/**
Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

User and Developer License

1. You can use this Software for both Personal use as well as Commercial use, 
with or without paying fee to Dilshad Mustafa. Please read fully below for the 
terms and conditions. You may use this Software only if you comply fully with 
all the terms and conditions of this License. 

2. If you want to redistribute this Software, you should redistribute only the 
original source code from Dilshad Mustafa's project and/or the compiled object 
binary form of the original source code and you should ensure to bundle and 
display this original license text and Dilshad Mustafa's copyright notice along 
with it as well as in each source code file of this Software. 

3. If you want to embed this Software within your work, you should embed only the 
original source code from Dilshad Mustafa's project and/or the compiled object 
binary form of the original source code and you should ensure to bundle and display 
this original license text and Dilshad Mustafa's copyright notice along with it as 
well as in each source code file of this Software. 

4. You should not modify this Software source code and/or its compiled object binary 
form in any way.

5. You should not redistribute any modified source code of this Software and/or 
its compiled object binary form with any changes, additions, enhancements, 
updates or modifications. You should not redistribute any modified works of this 
Software. You should not create and/or redistribute any straight forward 
translation and/or implementation of this Software source code to same and/or 
another programming language, either partially or fully. You should not redistribute 
embedded modified versions of this Software source code and/or its compiled object 
binary in any form, both within as well as outside your organization, company, 
legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. Under differently named or renamed software, you should not redistribute this 
Software and/or any modified works of this Software, including its source code 
and/or its compiled object binary form. Under your name or your company name or 
your product name, you should not publish this Software, including its source code 
and/or its compiled object binary form, modified or original. 

8. You agree to use the original source code from Dilshad Mustafa's project only
and/or the compiled object binary form of the original source code.

9. You accept and agree fully to the terms and conditions of this License of this 
software product, under same software name and/or if it is renamed in future.

10. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

11. The Copyright holder of this Software reserves the right to change the terms 
and conditions of this license without giving prior notice.

12. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.cass;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.dilmus.dilshad.scabi.common.DMJson;

import com.dilmus.dilshad.scabi.common.DMNamespace;
import com.dilmus.dilshad.scabi.common.DMNamespaceHelper;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DTable;
import com.dilmus.dilshad.scabi.ms.DMComputeServer;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.dilmus.dilshad.scabi.common.DMUtil;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dilshad Mustafa
 *
 */

@Path("/")
public class DMCassMetaServer extends Application {

	private static Logger log = null;
	
	private static DDB m_ddb = null;
	private static DMCassHelper m_cmh = null;
	private static DMNamespaceHelper m_dnh = null;
	private static boolean m_firstTime = true;
	private Set<Class<?>> st = new HashSet<Class<?>>();
	private static Integer m_syncObj = new Integer(0);
	
	public DMCassMetaServer() throws DScabiException {
		//classesSet.add(ApplicationCommandResource.class);
		st.add(DMCassMetaServer.class);
		
	}
	 
	public Set<Class<?>> getClasses() {
		return st;
	}
	
	public static int initialize() throws DScabiException {
	   	synchronized(m_syncObj) {
	   		if (m_firstTime) {
				//m_ddb = new DDB("localhost", "27017", "MetaDB");
			   	m_cmh = new DMCassHelper(m_ddb);
			   	m_dnh = new DMNamespaceHelper(m_ddb);
			   	m_firstTime = false;
			   	log.debug("initialize() called");
	   		}
		}
		return 0;
	}
	
   @GET
   @Path("/")
   public String get() { return "hello world from MetaServer"; }

   @POST
   @Path("/Meta/Compute/Alloc")
   public String computeAlloc(String request) throws DScabiException {
	   	DMComputeServer cm;
	   	try {
		   	synchronized(m_cmh) {
		   		cm = m_cmh.alloc();
		   		// // cm.updateStatus("Inuse");
		   		// this status update request should come from Compute server only
		   	}
	   	} catch (Error | RuntimeException e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Exception e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Throwable e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	}
	   	return cm.toString();

   }
   
   @POST
   @Path("/Meta/Compute/Register")
   public String computeRegister(String request) throws DScabiException, IOException {
	   log.debug("request : {}", request);
	   try {
		   synchronized(m_cmh) {
			   m_cmh.checkIfRunningAndRemove();
			   DMJson djson = new DMJson(request);
			   m_cmh.register(djson.getString("ComputeHost"), djson.getString("ComputePort"), djson.getString("MAXCSTHREADS"));
		   }
	   } catch (Error | RuntimeException e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   } catch (Exception e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   } catch (Throwable e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   }
   
	   return DMJson.ok();
   
   }

   @POST
   @Path("/Meta/isRunning")
   public String isRunning(String request) throws DScabiException {
	   	return DMJson.ok();
   
   }

   @POST
   @Path("/Meta/Compute/GetMany")
   public String getComputeMany(String request) throws DScabiException {
	   	LinkedList<DMComputeServer> cma = null;
	   	DMJson djson = null;
	   	// Previous works DMJson djsonResponse = null;
	   	long howMany = 0;
	   	String result = null;
	   	try {
	   		djson = new DMJson(request);
	   		howMany = Long.parseLong(djson.getString("GetComputeMany"));
	   	
		   	synchronized(m_cmh) {
		   		// Previous works cma = m_cmh.getMany(howMany);
		   		result = m_cmh.getMany(howMany);
		   		// status update request for each Compute Server should come from Compute server only
		   	}
		   	
		   	// Previous works djsonResponse = new DMJson("Count", "" + cma.size());
		   	/* Previous works
		   	djsonResponse = new DMJson();
		   	int k = 1;
		   	for (DMComputeServer cm : cma) {
		   		log.debug("cm.toString() : {}", cm.toString());
		   		djsonResponse = djsonResponse.add("" + k, cm.toString());
		   		k++;
		   	}
		   	djsonResponse.add("Count", "" + (k - 1));
		   	*/
	   	} catch (Error | RuntimeException e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
		} catch (Exception e) {
			return DMJson.error(DMUtil.serverErrMsg(e));
		} catch (Throwable e) {
			return DMJson.error(DMUtil.serverErrMsg(e));
		}
	   	// Previous works log.debug("djsonResponse.toString() : {}", djsonResponse.toString());
	   	// Previous works return djsonResponse.toString();
	   	
	   	log.debug("getComputeMany() result : {}", result);
	   	return result;
   }

   @POST
   @Path("/Meta/Compute/GetManyMayExclude")
   public String getComputeManyMayExclude(String request) throws DScabiException {
	   	LinkedList<DMComputeServer> cma = null;
	   	DMJson djson = null;
	   	// Previous works DMJson djsonResponse = null;
	   	long howMany = 0;
	   	String jsonStrExclude = null;
	   	String result = null;
	   	try {
	   		djson = new DMJson(request);
	   		howMany = Long.parseLong(djson.getString("GetComputeMany"));
	   		jsonStrExclude = djson.getString("ComputeExclude");
		   	synchronized(m_cmh) {
		   		// Previous works cma = m_cmh.getManyMayExclude(howMany, jsonStrExclude);
		   		result = m_cmh.getManyMayExclude(howMany, jsonStrExclude);
		   		// status update request for each Compute Server should come from Compute server only
		   	}
		   	
		   	// Previous works djsonResponse = new DMJson("Count", "" + cma.size());
		   	/* Previous works
		   	djsonResponse = new DMJson();
		   	int k = 1;
		   	for (DMComputeServer cm : cma) {
		   		log.debug("cm.toString() : {}", cm.toString());
		   		djsonResponse = djsonResponse.add("" + k, cm.toString());
		   		k++;
		   	}
		   	djsonResponse.add("Count", "" + (k - 1));
		   	*/
	   	} catch (Error | RuntimeException e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
		} catch (Exception e) {
			return DMJson.error(DMUtil.serverErrMsg(e));
		} catch (Throwable e) {
			return DMJson.error(DMUtil.serverErrMsg(e));
		}
	   	// Previous works log.debug("djsonResponse.toString() : {}", djsonResponse.toString());
	   	// Previous works return djsonResponse.toString();

	   	log.debug("getComputeManyMayExclude() result : {}", result);
	   	return result;
   }
   
   @POST
   @Path("/Meta/Namespace/Register")
   public String namespaceRegister(String request) throws DScabiException, IOException {
	   log.debug("request : {}", request);
	   try {
		   synchronized(m_dnh) {
			   DMJson dmjson = new DMJson(request);
			   String uuid = m_dnh.register(dmjson);
			   return DMJson.result(uuid);
		   }
	   } catch (Error | RuntimeException e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   } catch (Exception e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   } catch (Throwable e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   }
   
   }

   @POST
   @Path("/Meta/Namespace/isExist")
   public String namespaceExists(String request) throws DScabiException, IOException {
	   log.debug("request : {}", request);
	   try {
	   		log.debug("namespaceExists() request : {}", request);
	   		DMJson djson = new DMJson(request);
	   		synchronized(m_dnh) {
	   			boolean isExist = m_dnh.namespaceExists(djson.getString("Namespace"));
	   			log.debug("isExist : {}", isExist);
	   			if (isExist)
	   				return DMJson.asTrue();
	   			else
	   				return DMJson.asFalse();
	   		}
	   } catch (Error | RuntimeException e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   } catch (Exception e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   } catch (Throwable e) {
		   return DMJson.error(DMUtil.serverErrMsg(e));
	   }
   
   }

   @POST
   @Path("/Meta/Namespace/FindOne")
   public String findOneNamespace(String request) throws DScabiException {
	   	DMNamespace dname;
	   	
	   	try {
	   		DMJson djson = new DMJson(request);
	   		synchronized(m_dnh) {
	   			dname = m_dnh.findOneNamespace(djson.getString("Type"));
	   		}
	   	} catch (Error | RuntimeException e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Exception e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Throwable e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	}
	   	
	   	return dname.toString();
   
   }

   @POST
   @Path("/Meta/Namespace/Get")
   public String getNamespace(String request) throws DScabiException {
	   	DMNamespace dname;
	   	try {
	   		log.debug("getNamespace() request : {}", request);
	   		DMJson djson = new DMJson(request);
	   		synchronized(m_dnh) {
	   			dname = m_dnh.getNamespace(djson.getString("Namespace"));
	   		}
	   	} catch (Error | RuntimeException e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Exception e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Throwable e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	}
	   		
	   	return dname.toString();
   
   }
   
   
   @POST
   @Path("/Meta/Namespace/GetByQuery")
   public String getNamespaceByQuery(String request) throws DScabiException {
	   	DMNamespace dname;
	   	try {
	   		log.debug("getNamespace() request : {}", request);
	   		DMJson djson = new DMJson(request);
	   		synchronized(m_dnh) {
	   			dname = m_dnh.getNamespaceByJsonStrQuery(djson.getString("Namespace"), djson.getString("Type"));
	   		}
	   	} catch (Error | RuntimeException e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Exception e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	} catch (Throwable e) {
	   		return DMJson.error(DMUtil.serverErrMsg(e));
	   	}
	   	
	   	return dname.toString();
   
   }

   
   public static void main(String[] args) throws Exception 
   {
       System.out.println("Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.");

	   int port = 5000;
	   String dbHost = "localhost";
	   String dbPort = "27017";
	   boolean debug = false;
	   
	   if (4 == args.length) {
		   port = Integer.parseInt(args[0]);
		   dbHost = args[1];
		   dbPort = args[2];
		   System.out.println("Port : " + port);
		   System.out.println("dbHost : " + dbHost);
		   System.out.println("dbPort : " + dbPort);
		   if (args[3].equalsIgnoreCase("debug")) {
			   System.out.println("debug enabled");
			   debug = true;
		   } else {
			   System.out.println("Unrecognized commandline argument " + args[3] + " Exiting.");
			   System.out.println("Usage : <MetaServer_Port> <Database_HostName> <Database_Port> [debug]");
			   System.out.println("Usage : <MetaServer_Port> [debug]");
			   System.out.println("Usage : <No arguments> to use default settings");
			   return;
		   }
	   } else if (3 == args.length) {
		   port = Integer.parseInt(args[0]);
		   dbHost = args[1];
		   dbPort = args[2];
		   System.out.println("Port : " + port);
		   System.out.println("dbHost : " + dbHost);
		   System.out.println("dbPort : " + dbPort);
	   } else if (2 == args.length) {
		   port = Integer.parseInt(args[0]);
		   System.out.println("Port : " + port);
		   if (args[1].equalsIgnoreCase("debug")) {
			   System.out.println("debug enabled");
			   debug = true;
		   } else {
			   System.out.println("Unrecognized commandline argument " + args[1] + " Exiting.");
			   System.out.println("Usage : <MetaServer_Port> [debug]");
			   System.out.println("Usage : <MetaServer_Port> <Database_HostName> <Database_Port> [debug]");
			   System.out.println("Usage : <No arguments> to use default settings");
			   return;
		   }
	   } else if (1 == args.length) {
		   if (args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("-help")
				   || args[0].equalsIgnoreCase("--h") || args[0].equalsIgnoreCase("--help")) {
			   System.out.println("Usage : <MetaServer_Port> <Database_HostName> <Database_Port> [debug]");
			   System.out.println("Usage : <MetaServer_Port> [debug]");
			   System.out.println("Usage : <No arguments> to use default settings");
		   }
		   else {
			   port = Integer.parseInt(args[0]);
			   System.out.println("Port : " + port);
		   }
	   }
	   
       System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
       System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
       System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
       System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
       if (debug)
    	   System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
       System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
       //System.setProperty("org.slf4j.simplelogger.defaultlog", "debug");
   	   //System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
       final Logger log = LoggerFactory.getLogger(DMCassMetaServer.class);
       DMCassMetaServer.log = log;

       createDatabasesIfAbsent(dbHost, dbPort);
       m_ddb = new DDB(dbHost, dbPort, "MetaDB");
       createTablesIfAbsent();
       populateDataIfAbsent();
       initialize();
       // works m_cmh.removeAll();
	   m_cmh.checkIfRunningAndRemove();
       
       ServletHolder sh = new ServletHolder(HttpServletDispatcher.class);
       sh.setInitParameter("javax.ws.rs.Application", DMCassMetaServer.class.getCanonicalName()); 
       Server server = new Server(port);
       ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
       context.setContextPath("/");
       server.setHandler(context);
       context.addServlet(sh, "/*");
       server.start();

       log.info("MetaServer started");
       log.info("Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.");
       
       
   }
	
  private static int createDatabasesIfAbsent(String dbHost, String dbPort) throws DScabiException {
	   
	  MongoClient mongo = new MongoClient(dbHost, Integer.parseInt(dbPort));
	  MongoDatabase mongodb = mongo.getDatabase("MetaDB");	
	  MongoDatabase mongodb2 = mongo.getDatabase("AppTableDB");
	  MongoDatabase mongodb3 = mongo.getDatabase("JavaFileDB");
	  MongoDatabase mongodb4 = mongo.getDatabase("FileDB");
	  log.debug("mongodb.getName() : {}", mongodb.getName());
	  log.debug("mongodb2.getName() : {}", mongodb2.getName());
	  log.debug("mongodb3.getName() : {}", mongodb3.getName());
	  log.debug("mongodb4.getName() : {}", mongodb4.getName());

	  mongo.close();
	  return 0;
   }
  
   private static int createTablesIfAbsent() throws DScabiException {
	   
	   if (false == m_ddb.tableExists("ComputeMetaDataTable")) {
		   m_ddb.createTable("ComputeMetaDataTable");
	   }
	   if (false == m_ddb.tableExists("NamespaceTable")) {
		   m_ddb.createTable("NamespaceTable");
	   }
   
	   return 0;
   }
   
   private static int populateDataIfAbsent() throws DScabiException, IOException {
		DTable t = m_ddb.getTable("NamespaceTable");
		if (t.count() != 0)
			return 0;

		String uuid1 = UUID.randomUUID().toString();
		String uuid2 = UUID.randomUUID().toString();
		String uuid3 = UUID.randomUUID().toString();
		String uuid4 = UUID.randomUUID().toString();
		String uuid5 = UUID.randomUUID().toString();
		Date d= new Date();
		String sdate = d.toString();
		String jsonRow1 = "{ \"Namespace\" : \"MyOrg.Meta1\", \"Type\" : \"MetaThis\", \"Host\" : \"localhost\", \"Port\" : \"4567\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"MetaServer\", \"SystemType\" : \"MetaServer\", \"SystemUUID\" : \"" + uuid1 + "\", \"RegisteredDate\" : \"" + sdate + "\", \"Status\" : \"Available\", \"StatusDate\" : \"" + sdate + "\" }";
		String jsonRow2 = "{ \"Namespace\" : \"MyOrg.Meta2\", \"Type\" : \"MetaRemote\", \"Host\" : \"localhost\", \"Port\" : \"4567\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"MetaServer\",  \"SystemType\" : \"MetaServer\", \"SystemUUID\" : \"" + uuid2 + "\", \"RegisteredDate\" : \"" + sdate + "\", \"Status\" : \"Available\", \"StatusDate\" : \"" + sdate + "\" }";
		String jsonRow3 = "{ \"Namespace\" : \"MyOrg.MyTables\", \"Type\" : \"AppTable\", \"Host\" : \"localhost\", \"Port\" : \"27017\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"AppTableDB\", \"SystemType\" : \"MongoDB\", \"SystemUUID\" : \"" + uuid3 + "\", \"RegisteredDate\" : \"" + sdate + "\", \"Status\" : \"Available\", \"StatusDate\" : \"" + sdate + "\" }";
		String jsonRow4 = "{ \"Namespace\" : \"MyOrg.MyJavaFiles\", \"Type\" : \"JavaFile\", \"Host\" : \"localhost\", \"Port\" : \"27017\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"JavaFileDB\", \"SystemType\" : \"MongoDB\", \"SystemUUID\" : \"" + uuid4 + "\", \"RegisteredDate\" : \"" + sdate + "\", \"Status\" : \"Available\", \"StatusDate\" : \"" + sdate + "\" }";
		String jsonRow5 = "{ \"Namespace\" : \"MyOrg.MyFiles\", \"Type\" : \"File\", \"Host\" : \"localhost\", \"Port\" : \"27017\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"FileDB\",  \"SystemType\" : \"MongoDB\", \"SystemUUID\" : \"" + uuid5 + "\", \"RegisteredDate\" : \"" + sdate + "\", \"Status\" : \"Available\", \"StatusDate\" : \"" + sdate + "\" }";
				
		String jsonCheck = "{ \"Namespace\" : \"1\" }";
		
		t.insertRow(jsonRow1, jsonCheck);
		t.insertRow(jsonRow2, jsonCheck);
		t.insertRow(jsonRow3, jsonCheck);
		t.insertRow(jsonRow4, jsonCheck);
		t.insertRow(jsonRow5, jsonCheck);
	   
	   return 0;
   }
}
