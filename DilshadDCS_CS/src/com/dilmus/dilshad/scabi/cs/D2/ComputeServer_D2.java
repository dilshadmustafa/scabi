/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 21-Jan-2016
 * File Name : ComputeServer_D2.java
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

package com.dilmus.dilshad.scabi.cs.D2;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

import org.apache.ftpserver.FtpServerConfigurationException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;
import bsh.ParseException;
import bsh.TargetError;
//import spark.Request;
//import spark.Response;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;
import javassist.Modifier;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMJsonHelper;
import com.dilmus.dilshad.scabi.common.DMNamespace;
import com.dilmus.dilshad.scabi.common.DMNamespaceHelper;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DaoHelper;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.deprecated.DDBOld;
import com.dilmus.dilshad.scabi.deprecated.Dao2;
import com.dilmus.dilshad.storage.IStorageHandler;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DMComputeTemplate;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.mongodb.DBCollection;

import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataPartition;
import com.dilmus.dilshad.scabi.core.DataUnit;

// for BigInteger
import java.math.*;
import java.net.BindException;
import java.net.InetAddress;

/**
 * @author Dilshad Mustafa
 *
 */

@Path("/")
public class ComputeServer_D2 extends Application {

	private static Logger log = null;
	
	private Set<Class<?>> st = new HashSet<Class<?>>();

	public static final HashMap<String, String> m_taskIdStatusMap = new HashMap<String, String>();
	public static final HashMap<String, Future<?>> m_taskIdFutureMap = new HashMap<String, Future<?>>();
	public static final HashMap<String, String> m_taskIdResultMap = new HashMap<String, String>();
	public static final HashMap<String, DataPartition> m_partitionIdDataPartitionMap = new HashMap<String, DataPartition>();
	public static final HashMap<String, IStorageHandler> m_splitAppIdIStorageHandlerMap = new HashMap<String, IStorageHandler>();	
	
	public static ExecutorService m_threadPool = null;
	
	public static final String S_TASKID_ENTERED = "1";
	// Not needed public static final String S_FUTURE_CREATED = "2";
	public static final String S_EXECUTION_STARTED = "2";
	public static final String S_EXECUTION_COMPLETED = "3";
	public static final String S_EXECUTION_ERROR = "4";
	public static final String S_RESULT_SENT = "5";
	
	// "CSR.SYM.RUN" - from Compute Server System, Runnable run() code
	// "CSR.SYM.RET" - from Compute Server System, REST call code
	// "CSR.SYM.EXE" - from Compute Server System, DMExecute method code
	// "CSR.SYM.APP" - from Compute Server System, User App code
	
	private static String m_localDirPath = null;
	// Previous works private static String m_storageDirPath = null;
	
	private static String m_storageProvider = null;
	private static String m_mountDirPath = null;
	private static String m_storageConfig = null;
	
	public static String getLocalDirPath() {
		return m_localDirPath;
	}
	
	public static String getStorageDirPath() throws DScabiException {
		return m_mountDirPath;
		// Previous works return m_storageDir;
	}
	
	public static String getStorageProvider() throws DScabiException {
		return m_storageProvider;
	}
	
	public static String getStorageConfig() throws DScabiException {
		return m_storageConfig;
	}
	
	public static int closeDataPartitionsForAppIdSU(String appId, long splitUnit) throws IOException {
		
		LinkedList<DataPartition> dpList = new LinkedList<DataPartition>();
		LinkedList<String> partitionList = new LinkedList<String>();
		
		// Prefix "_" is required to prevent incorrect matches like "11_<appId>", "21_<appId>", etc 
		// matching with "1_<appId>"
		String search = "_" + splitUnit + "_" + appId.replace("_", "");
		
		synchronized (m_partitionIdDataPartitionMap) {
			Set<String> keys = m_partitionIdDataPartitionMap.keySet();
			for (String s : keys) {
				if (s.contains(search)) {
					log.debug("closeDataPartitionsForAppId() partitionId matches : {}", s);
					DataPartition dp = m_partitionIdDataPartitionMap.get(s);
					dpList.add(dp);
					partitionList.add(s);
				}
			}
		}
		
		// dp.close() internally calls dp.flushFiles() and hence may be slow
		// hence this call is outside the synchronized (m_partitionIdDataPartitionMap) code block
		for (DataPartition dp : dpList) {
			dp.close();
		}
		
		synchronized (m_partitionIdDataPartitionMap) {
			for (String s : partitionList) {
				m_partitionIdDataPartitionMap.remove(s);
			}
		}
	
		return 0;
	}
	
	public ComputeServer_D2() throws DScabiException {
		//classesSet.add(ApplicationCommandResource.class);
		st.add(ComputeServer_D2.class);

	}
	 
	public Set<Class<?>> getClasses() {
		return st;
	}
	
   @GET
   @Path("/")
   public String get() { return "hello world from ComputeServer"; }

   @POST
   @Path("/")
   public String post(String s) { return "hello world from ComputeServer"; }

   @POST
   @Path("/Compute/isRunning")
   public String isRunning(String request) {
	   
	   return DMJson.ok();
   }
   
   @POST
   @Path("/Task/IsDoneProcessing")
   public String isDoneProcessing(String request) {
	   log.debug("isDoneProcessing() request : {}", request);
	   String taskId = null;
	   DMJson djson = null;
	   boolean check = false;
	   Future<?> f = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskId = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { 
			   check = m_taskIdStatusMap.containsKey(taskId);
		   }
		   if (false == check) {
			   return DMJson.error("CSE.CSE.IDG.1", "isDoneProcessing() Task Id " + taskId + " is not found in taskId-Status Map");
		   }
		   synchronized(m_taskIdFutureMap) { 
			   f = m_taskIdFutureMap.get(taskId); 
		   }
		   if (null == f) {
			   return DMJson.error("CSE.CSE.IDG.2", "isDoneProcessing() For Task Id " + taskId + ", Future is null in taskId-Future Map");
		   }
		   if (f.isDone())
			   return DMJson.asTrue();
		   else
			   return DMJson.asFalse();
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.IDG.3", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.IDG.4", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSE.CSE.IDG.5", DMUtil.serverErrMsg(e));
	   }

   }

   @POST
   @Path("/Task/RetrieveResult")
   public String retrieveResult(String request) {
	   log.debug("retrieveResult() request : {}", request);
	   String taskId = null;
	   String result = null;
	   DMJson djson = null;
	   boolean check = false;
	   Future<?> f = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskId = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { 
			   check = m_taskIdStatusMap.containsKey(taskId);
		   }
		   if (false == check) {
			   return DMJson.error("CSE.CSE.RRT.1", "retrieveResult() Task Id " + taskId + " is not found in taskId-Status Map");
		   }
		   synchronized(m_taskIdFutureMap) { 
			   f = m_taskIdFutureMap.get(taskId); 
		   }
		   if (null == f) {
			   return DMJson.error("CSE.CSE.RRT.2", "retrieveResult() For Task Id " + taskId + ", Future is null in taskId-Future Map");
		   }
		   f.get();
		   synchronized(m_taskIdResultMap) { 
			   result = m_taskIdResultMap.get(taskId);
		   }
		   if (null == result) {
			   return DMJson.error("CSE.CSE.RRT.3", "retrieveResult() For Task Id " + taskId + " result is null in taskId-Result Map");
		   }	
		   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_RESULT_SENT); }
		   synchronized(ComputeServer_D2.m_taskIdFutureMap) { ComputeServer_D2.m_taskIdFutureMap.remove(taskId); }
		   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.remove(taskId); }
		   return result;
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.RRT.4", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.RRT.5", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSE.CSE.RRT.6", DMUtil.serverErrMsg(e));
	   }

   }   
   
   @POST
   @Path("/Data/Execute/ExecuteForDataUnit")
   public String dataExecuteForDataUnitOperators(String request) {
	   log.debug("dataExecuteForDataUnitOperators() request : {}", request);
	   String taskID = null;
	   String result = null;
	   DMJson djson = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskID = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { m_taskIdStatusMap.put(taskID, S_TASKID_ENTERED); }
		   final String req = request;
		   final String taskId = taskID;
		   Runnable task = new Runnable() {
			   public void run() {
				   
				   String result = null;
				   ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
				   
				   try {
					   DMJson dj = new DMJson(request);					   
					   String appId = dj.getString("AppId");
					   long splitUnit = dj.getCU();
					   DMClassLoader dcl = new DMClassLoader();
					   
					   if(false == dj.contains("AddJars")) {
						   log.debug("run() Information only : No additional jars to load. AddJars are not provided.");
						   Thread.currentThread().setContextClassLoader(dcl);
					   } else {
						   log.debug("run() AddJars are provided");
						   DMExecute.loadAddJars(dcl, dj);
						   
						   //=====Debugging purpose only==========
						   //log.debug("run() Going to load class TestNew");
						   //Class<?> testNew = dcl.loadClass("test.TestNew");
						   //log.debug("run() Loaded class TestNew with name : {}", testNew.getName());
						   //=====================================
						   
						   Thread.currentThread().setContextClassLoader(dcl);
					   }
					   
					   long startCommandId = dj.getLongOf("StartCommandId");
					   log.debug("run() startCommandId : {}", startCommandId);
					   if (1 == startCommandId) {
						   DMExecute.dataExecuteForDataUnit(dcl, req, dj);
					   }
					   if (ComputeServer_D2.S_EXECUTION_ERROR == ComputeServer_D2.m_taskIdStatusMap.get(taskId)) {
						   Thread.currentThread().setContextClassLoader(originalLoader);
						   return;
					   }
					   DMExecute.dataExecuteForOperators(dcl, req, dj);
					   if (ComputeServer_D2.S_EXECUTION_ERROR == ComputeServer_D2.m_taskIdStatusMap.get(taskId)) {
						   Thread.currentThread().setContextClassLoader(originalLoader);
						   return;
					   }
					   closeDataPartitionsForAppIdSU(appId, splitUnit);
					   Thread.currentThread().setContextClassLoader(originalLoader);
					   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
				   } catch (Error | RuntimeException e) {
			  			result = DMJson.error("CSE.RUN.RUN.1", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				   } catch(Exception e) {
			  			result = DMJson.error("CSE.RUN.RUN.2", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				   } catch(Throwable e) {
			  			result = DMJson.error("CSE.RUN.RUN.3", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				   }
			   }
		   };
		   Future<?> f = m_threadPool.submit(task);
		   synchronized(m_taskIdFutureMap) { m_taskIdFutureMap.put(taskID, f); }
		   return DMJson.ok();
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.DEFD.1", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.DEFD.2", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSE.CSE.DEFD.3", DMUtil.serverErrMsg(e));
	   }
   }
   
   
   @POST
   @Path("/Compute/Execute/BshCode")
   public String computeExecuteCode(String request) {
	   //log.debug("computeExecuteCode() request : {}", request);
	   String taskId = null;
	   String result = null;
	   DMJson djson = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskId = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { m_taskIdStatusMap.put(taskId, S_TASKID_ENTERED); }
		   // Previous works result = DMExecute.computeExecuteCode(djson);
		   final String req = request;
		   final DMJson dj = djson;
		   Runnable task = new Runnable() {
			   public void run() {
				   String taskId = dj.getString("TaskId");
				   String result = null;
				   try {
					   DMExecute.computeExecuteCode(req);
				   } catch (Error | RuntimeException e) {
			  			result = DMJson.error("CSE.RUN.RUN.1", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Exception e) {
			  			result = DMJson.error("CSE.RUN.RUN.2", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Throwable e) {
			  			result = DMJson.error("CSE.RUN.RUN.3", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   }
			   }
		   };
		   Future<?> f = m_threadPool.submit(task);
		   synchronized(m_taskIdFutureMap) { m_taskIdFutureMap.put(taskId, f); }
		   return DMJson.ok();
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.CEC.1", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.CEC.2", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSE.CSE.CEC.3", DMUtil.serverErrMsg(e));
	   }

   	
   }

   @POST
   @Path("/Compute/Execute/Class")
   public String computeExecuteClass(String request) {
	   //log.debug("computeExecuteClass() request : {}", request);
	   String taskId = null;
	   String result = null;
	   DMJson djson = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskId = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { m_taskIdStatusMap.put(taskId, S_TASKID_ENTERED); }
		   // Previous works result = DMExecute.computeExecuteClass(djson);
		   final String req = request;
		   final DMJson dj = djson;
		   Runnable task = new Runnable() {
			   public void run() {
				   String taskId = dj.getString("TaskId");
				   String result = null;
				   try {
					   DMExecute.computeExecuteClass(req);
				   } catch (Error | RuntimeException e) {
			  			result = DMJson.error("CSE.RUN.RUN.1", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Exception e) {
			  			result = DMJson.error("CSE.RUN.RUN.2", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Throwable e) {
			  			result = DMJson.error("CSE.RUN.RUN.3", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   }
			   }
		   };
		   Future<?> f = m_threadPool.submit(task);
		   synchronized(m_taskIdFutureMap) { m_taskIdFutureMap.put(taskId, f); }
		   return DMJson.ok();
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.CEC2.1", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.CEC2.2", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSE.CSE.CEC2.3", DMUtil.serverErrMsg(e));
	   }
   
   }

   @POST
   @Path("/Compute/Execute/ClassFromObject")
   public String computeExecuteFromObject(String request) {
	   //log.debug("computeExecuteFromObject() request : {}", request);
	   String taskId = null;
	   String result = null;
	   DMJson djson = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskId = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { m_taskIdStatusMap.put(taskId, S_TASKID_ENTERED); }
		   // Previous works result = DMExecute.computeExecuteFromObject(djson);
		   final String req = request;
		   final DMJson dj = djson;
		   Runnable task = new Runnable() {
			   public void run() {
				   String taskId = dj.getString("TaskId");
				   String result = null;
				   try {
					   DMExecute.computeExecuteFromObject(req);
				   } catch (Error | RuntimeException e) {
			  			result = DMJson.error("CSE.RUN.RUN.1", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Exception e) {
			  			result = DMJson.error("CSE.RUN.RUN.2", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Throwable e) {
			  			result = DMJson.error("CSE.RUN.RUN.3", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   }
			   }
		   };
		   Future<?> f = m_threadPool.submit(task);
		   synchronized(m_taskIdFutureMap) { m_taskIdFutureMap.put(taskId, f); }
		   return DMJson.ok();
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.CEFO.1", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.CEFO.2", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSE.CSE.CEFO.3", DMUtil.serverErrMsg(e));
	   }
   
   }
   
   @POST
   @Path("/Compute/Execute/ClassNameInJar")
   public String computeExecuteClassNameInJar(String request) {
	   //log.debug("computeExecuteClassNameInJar() request : {}", request);
	   String taskId = null;
	   String result = null;
	   DMJson djson = null;
	   
	   try {
		   djson = new DMJson(request);
		   taskId = djson.getString("TaskId");
		   synchronized(m_taskIdStatusMap) { m_taskIdStatusMap.put(taskId, S_TASKID_ENTERED); }
		   // Previous works result = DMExecute.computeExecuteClassNameInJar(djson);
		   final String req = request;
		   final DMJson dj = djson;
		   Runnable task = new Runnable() {
			   public void run() {
				   String taskId = dj.getString("TaskId");
				   String result = null;
				   try {
					   DMExecute.computeExecuteClassNameInJar(req);
				   } catch (Error | RuntimeException e) {
			  			result = DMJson.error("CSE.RUN.RUN.1", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Exception e) {
			  			result = DMJson.error("CSE.RUN.RUN.2", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   } catch(Throwable e) {
			  			result = DMJson.error("CSE.RUN.RUN.3", DMUtil.serverErrMsg(e));
					   	synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   }
			   }
		   };
		   Future<?> f = m_threadPool.submit(task);
		   synchronized(m_taskIdFutureMap) { m_taskIdFutureMap.put(taskId, f); }
		   return DMJson.ok();
	   } catch (Error | RuntimeException e) {
	   		return DMJson.error("CSE.CSE.CECJ.1", DMUtil.serverErrMsg(e));
	   } catch(Exception e) {
	   		return DMJson.error("CSE.CSE.CECJ.2", DMUtil.serverErrMsg(e));
	   } catch(Throwable e) {
	   		return DMJson.error("CSR.CSE.CECJ.3", DMUtil.serverErrMsg(e));
	   }
   
   }

   private static void init() {
	   
		m_localDirPath = System.getProperty("scabi.local.dir");
		if (null == m_localDirPath || m_localDirPath.length() == 0) {
			log.error("init() Local directory is not specified in property scabi.local.dir");
			System.exit(0);
		}
		log.debug("init() Property scabi.local.dir, m_localDirPath : {}", m_localDirPath);

		// scabi.local.dir=
		// scabi.storage.provider=dfs|fuse|nfs|seaweedfs
		// scabi.dfs.mount.dir=
		// scabi.fuse.mount.dir=
		// scabi.nfs.mount.dir=
		// scabi.seaweedfs.config="failover1_host-failover1_port;failover2_host-failover2_port"
		
		m_storageProvider = System.getProperty("scabi.storage.provider");
		if (m_storageProvider != null && m_storageProvider.length() > 0) {
	 		log.debug("init() Property scabi.storage.provider, m_storageProvider : {}", m_storageProvider);
			if (m_storageProvider.equalsIgnoreCase("dfs")) {
				m_mountDirPath = System.getProperty("scabi.dfs.mount.dir");
		 		if (null == m_mountDirPath || m_mountDirPath.length() == 0) {
		 			log.error("init() Mount or storage directory is not specified in property scabi.dfs.mount.dir");
		 			System.exit(0);
		 		}
		 		log.debug("init() Property scabi.dfs.mount.dir, m_mountDirPath : {}", m_mountDirPath);
			} else if (m_storageProvider.equalsIgnoreCase("seaweedfs")) {
				m_storageConfig = System.getProperty("scabi.seaweedfs.config");
		 		if (null == m_storageConfig || m_storageConfig.length() == 0) {
		 			log.error("init() Config is not specified in property scabi.seaweedfs.config");
		 			System.exit(0);
		 		}
		 		log.debug("init() Property scabi.seaweedfs.config, m_storageConfig : {}", m_storageConfig);
			} else {
	 			log.error("init() Invalid Storage Provider is specified in property scabi.storage.provider : {}", m_storageProvider);
	 			log.error("init() Valid Storage Provider values : dfs, fuse, nfs, seaweedfs");
	 			System.exit(0);
			}
		} else {
			log.error("init() Storage Provider is not specified in property scabi.storage.provider");
			System.exit(0);
		} 
	   
   }
   
   public static void main(String[] args) throws Exception 
   {
	   // works
	   // -Dscabi.local.dir="/home/anees/testdata/bigfile/tutorial/testlocal"
	   // -Dscabi.storage.provider="dfs"
	   // -Dscabi.dfs.mount.dir="/home/anees/testdata/bigfile/tutorial/teststorage"
	   
	   // -Dscabi.storage.provider="seaweedfs"
	   // -Dscabi.seaweedfs.config="localhost-8888"
	   // Not used -Dscabi.seaweedfs.config='{ "1" : "{ "Host" : "localhost", "Port" : "8888" }" }'
	   
       System.out.println("Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.");

	   int port = 0;
	   String metaHost = null;
	   String metaPort = null;
	   boolean debug = false;
	   int threads = 5;
	   int requestHandlers = 5;
	   
	   if (5 == args.length) {
		   port = Integer.parseInt(args[0]);
		   metaHost = args[1];
		   metaPort = args[2];
		   threads = Integer.parseInt(args[3]);
		   if (threads < 5)
			   threads = 5;
		   System.out.println("Port : " + port);
		   System.out.println("metaHost : " + metaHost);
		   System.out.println("metaPort : " + metaPort);
		   System.out.println("threads : " + threads);
		   if (args[4].equalsIgnoreCase("debug")) {
			   System.out.println("debug enabled");
			   debug = true;
		   } else {
			   System.out.println("Unrecognized commandline argument " + args[4] + " Exiting.");
			   System.out.println("Usage : <ComputeServer_Port> <MetaServer_HostName> <MetaServer_Port> [<NoOfThreads> [debug]]");
			   return;
		   }
	   } else if (4 == args.length) {
		   port = Integer.parseInt(args[0]);
		   metaHost = args[1];
		   metaPort = args[2];
		   threads = Integer.parseInt(args[3]);
		   if (threads < 5)
			   threads = 5;
		   System.out.println("Port : " + port);
		   System.out.println("metaHost : " + metaHost);
		   System.out.println("metaPort : " + metaPort);
		   System.out.println("threads : " + threads);
	   } else if (3 == args.length) {
		   port = Integer.parseInt(args[0]);
		   metaHost = args[1];
		   metaPort = args[2];
		   System.out.println("Port : " + port);
		   System.out.println("metaHost : " + metaHost);
		   System.out.println("metaPort : " + metaPort);
	   } else if (1 == args.length) {
		   if (args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("-help")
			   || args[0].equalsIgnoreCase("--h") || args[0].equalsIgnoreCase("--help")) {
			   System.out.println("Usage : <ComputeServer_Port> <MetaServer_HostName> <MetaServer_Port> [<NoOfThreads> [debug]]");
		   } else {
			   System.out.println("Incorrect number of arguments supplied.");
			   System.out.println("Usage : <ComputeServer_Port> <MetaServer_HostName> <MetaServer_Port> [<NoOfThreads> [debug]]");
		   }
	   } else {
		   System.out.println("Incorrect number of arguments supplied.");
		   System.out.println("Usage : <ComputeServer_Port> <MetaServer_HostName> <MetaServer_Port> [<NoOfThreads> [debug]]");
		   return;
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
 		
       //System.setProperty("org.dilmus.dilshad.scabi.Meta.Host", "localhost");
       //System.setProperty("org.dilmus.dilshad.scabi.Meta.Port", "5000");
       //System.setProperty("org.dilmus.dilshad.scabi.Meta.FailOver.Host", "localhost");
       //System.setProperty("org.dilmus.dilshad.scabi.Meta.FailOver.Port", "5000");
 		
       final Logger log = LoggerFactory.getLogger(ComputeServer_D2.class);
       ComputeServer_D2.log = log;

       	// -Duser.dir="/home/anees/testdata/bigfile/tutorial/"
 		// Previous works String userDirPath = DMUtil.getUserDir();
 		// Previous works log.debug("userDirPath : {}", userDirPath);

       init();
       
       //metaHost = System.getProperty("org.dilmus.scabi.Meta.Host");
       //metaPort = System.getProperty("org.dilmus.scabi.Meta.Port");
       
       DMeta meta = new DMeta(metaHost, metaPort);
       String hostName =  InetAddress.getLocalHost().getHostName();
       log.debug("hostName : {}", hostName);
       try {
    	   meta.computeRegister(hostName, args[0], "" + threads);
       } catch (Error | RuntimeException e) {
    	   log.error("Error registering this Compute Server");
    	   log.error("Error message dump : {}", DMUtil.serverErrMsg(e));
    	   return;
       } catch (Exception e) {
    	   log.error("Error registering this Compute Server");
    	   log.error("Error message dump : {}", DMUtil.serverErrMsg(e));
    	   return;
       } catch (Throwable e) {
    	   log.error("Error registering this Compute Server");
    	   log.error("Error message dump : {}", DMUtil.serverErrMsg(e));
    	   return;
       }
       
       /* works
       ServletHolder sh = new ServletHolder(HttpServletDispatcher.class);
       sh.setInitParameter("javax.ws.rs.Application", ComputeServer.class.getCanonicalName()); 
       try {
       //Server server = new Server(port);
       Server server = new Server(port);
       ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
       context.setContextPath("/");
       server.setHandler(context);
       context.addServlet(sh, "/*");
       server.start();
       } catch (Exception e) {
    	   log.debug("Exception : {}", e);
    	   System.exit(0);
       }
       */
       
       ComputeServer_D2.m_threadPool = Executors.newFixedThreadPool(threads);
       
       //===============================
       ServletHolder sh = new ServletHolder(HttpServletDispatcher.class);
       sh.setInitParameter("javax.ws.rs.Application", ComputeServer_D2.class.getCanonicalName()); 
       try {
    	   //Server server = new Server(port);
    	   QueuedThreadPool q = new QueuedThreadPool();
    	   q.setMinThreads(5);
    	   // Previous works q.setMaxThreads(threads);
    	   q.setMaxThreads(requestHandlers);
    	   //Error ComputeServer exits q.setDaemon(true);
    	   q.setIdleTimeout(Integer.MAX_VALUE); // 60000
      	   
    	   Server server = new Server(q);
       
    	   ServerConnector sc = new ServerConnector(server);
    	   sc.setIdleTimeout(Integer.MAX_VALUE); // 60000
    	   sc.setPort(port);
       
    	   ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    	   context.setContextPath("/");
    	   server.setHandler(context);
    	   context.addServlet(sh, "/*");
    	   
    	   server.addConnector(sc);
    	   server.start();
       } catch (Exception e) {
    	   log.debug("Exception : {}", e);
    	   System.exit(0);
       }
       //================================
       log.info("ComputeServer started");
       log.info("Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.");
       
       // log.info("FtpServer primary starting...");
       DMFtpServer ftpPrimary = null;
       try {
    	   // Future decision ftpPrimary = new DMFtpServer();
    	   // Future decision log.info("FtpServer primary started");
       } catch (FtpServerConfigurationException e) {
    	   if (e.getCause() != null) {
    		   if (e.getCause() instanceof BindException)
					log.debug("FtpServer error : Port may be already in use");
    		   else 
    			   throw e;
    	   } else
    		   throw e;
       }
       
       // for later implementation Runtime.getRuntime().addShutdownHook(hook);
   }
	
}
