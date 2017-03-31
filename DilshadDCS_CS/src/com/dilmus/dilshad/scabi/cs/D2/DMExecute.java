/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 24-Aug-2016
 * File Name : DMExecute.java
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMComputeTemplate;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMDataTemplate;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMOperatorTemplate;
import com.dilmus.dilshad.scabi.common.DMSeaweedStorageHandler;
import com.dilmus.dilshad.scabi.common.DMStdStorageHandler;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataPartition;
import com.dilmus.dilshad.scabi.core.DataUnit;
import com.dilmus.dilshad.scabi.core.DataElement;
import com.dilmus.dilshad.scabi.core.IOperator;
import com.dilmus.dilshad.scabi.core.IShuffle;
import com.dilmus.dilshad.scabi.core.data.DMShuffle;
import com.dilmus.dilshad.scabi.core.data.DataAsyncConfigNode;
import com.dilmus.dilshad.storage.IStorageHandler;

import bsh.EvalError;
import bsh.Interpreter;
import bsh.ParseException;
import bsh.TargetError;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;
import javassist.Modifier;
import javassist.NotFoundException;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMExecute {

	private static final Logger log = LoggerFactory.getLogger(DMExecute.class);
	private static final DMCounter M_DMCOUNTER = new DMCounter();

	private static final DMCounter m_gcCounter = new DMCounter();
	
	private static String getStorageAppIdDirPath(String storageDirPath, String appId) {
		
		appId = appId.replace("_", "");
		
		String appDirPath = null;
		if (storageDirPath.endsWith(File.separator)) {
			appDirPath = storageDirPath + appId;
		} else {
			appDirPath = storageDirPath + File.separator + appId;
		}
		
		log.debug("getStorageAppIdDirPath() Storage AppId Dir path  appDirPath : {}", appDirPath);
		
		return appDirPath;
	}
	
	public static int createDirs(String dirPath) throws DScabiException {
		
		DMUtil.createDirIfAbsent(dirPath);
		
		String dpPath = DMUtil.appendToDirPath(dirPath, "dp");
		DMUtil.createDirIfAbsent(dpPath);
		
		String fromPath = DMUtil.appendToDirPath(dirPath, "from");
		DMUtil.createDirIfAbsent(fromPath);
		
		String fromImportPath = DMUtil.appendToDirPath(dirPath, "from" + File.separator + "import");
		DMUtil.createDirIfAbsent(fromImportPath);
		
		String fromDpPath = DMUtil.appendToDirPath(dirPath, "from" + File.separator + "from_dp");
		DMUtil.createDirIfAbsent(fromDpPath);
		
		String toPath = DMUtil.appendToDirPath(dirPath, "to");
		DMUtil.createDirIfAbsent(toPath);
		
		String toExportPath = DMUtil.appendToDirPath(dirPath, "to" + File.separator + "export");
		DMUtil.createDirIfAbsent(toExportPath);
		
		String toDpPath = DMUtil.appendToDirPath(dirPath, "to" + File.separator + "to_dp");
		DMUtil.createDirIfAbsent(toDpPath);
		
		return 0;
	}
	
	private static void gc() {
		m_gcCounter.inc();
		if (m_gcCounter.value() >= 2500) {
			System.gc();
			m_gcCounter.set(0);
		}
	}
	
	public static String dataExecuteForDataUnit(DMClassLoader dcl, String request, DMJson dj) throws IOException {
		gc();
		
		ClassPool pool = null;
		String result = null;
		String taskId = null;
		DataPartition dp = null;
		
		// log.debug("dataExecuteForDataUnit() request : {}", request);
		try {
			taskId = dj.getString("TaskId");
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }

			long configCount = dj.getLong("ConfigCount");
		   
		   	//for (long i = 1; i <= configCount; i++) {

			String jsonStr = dj.getString("1");
			DMJson djson = new DMJson(jsonStr);
			String commandId = djson.getString("CommandId");
			log.debug("dataExecuteForDataUnit() CommandId : {}", commandId);
			
			if (false == commandId.equals("1")) {
				throw new DScabiException("CommandId is not 1 for first ConfigNode entry in Json", "EXE.DEFD.1");
			}
			
			String isDataInitiatorProvided = djson.getString("IsDataInitiatorProvided");
			log.debug("dataExecuteForDataUnit() isDataInitiatorProvided : {}", isDataInitiatorProvided);
			
			long retryNumber = dj.getLongOf("RetryNumber");
			log.debug("dataExecuteForDataUnit() retryNumber : {}", retryNumber);
			long maxRetry = dj.getLongOf("MaxRetry");
			log.debug("dataExecuteForDataUnit() maxRetry : {}", maxRetry);
	  		String appId = dj.getString("AppId");
			log.debug("dataExecuteForDataUnit() AppId : {}", appId);
			long parallelNumber = dj.getLongOf("ParallelNumber");
			log.debug("dataExecuteForDataUnit() parallelNumber : {}", parallelNumber);
			long maxParallel = dj.getLongOf("MaxParallel");
			log.debug("dataExecuteForDataUnit() maxNumber : {}", maxParallel);
			long startCommandId = dj.getLongOf("StartCommandId");
			log.debug("dataExecuteForDataUnit() startCommandId : {}", startCommandId);
			long endCommandId = dj.getLongOf("EndCommandId");
			log.debug("dataExecuteForDataUnit() endCommandId : {}", endCommandId);
			
	  		DataContext dctx1 = new DataContext("TotalComputeUnit", dj.getString("TotalComputeUnit"));
	  		dctx1.add("SplitComputeUnit", dj.getString("SplitComputeUnit"));
	  		// cw dctx1.add("AppId", dj.getString("AppId"));
	  		dctx1.add("JobId", dj.getString("JobId"));
	    	dctx1.add("ConfigId", dj.getString("ConfigId"));
	    	dctx1.add("TaskId", dj.getString("TaskId"));
	    	dctx1.add("JsonInput", djson.getString("JsonInput"));
	    	dctx1.setRetryNumber(retryNumber);
	    	dctx1.setMaxRetry(maxRetry);
	    	dctx1.setParallelNumber(parallelNumber);
	    	dctx1.setMaxParallel(maxParallel);
	    	dctx1.setAppId(appId);
	    	
			DMJson djsonBy = new DMJson();
			djsonBy.add("AppId", dctx1.getAppId());
			djsonBy.add("SplitUnit", "" + dctx1.getDU());
			djsonBy.add("RetryNumber", "" + dctx1.getRetryNumber());
			djsonBy.add("ParallelNumber", "" + dctx1.getParallelNumber());
			String createdBy = djsonBy.toString();
	    	
	  		String localDPDirPath = ComputeServer_D2.getLocalDirPath();
	  		String localDPDirPathForThisAppId = null;
			
	  		if (localDPDirPath.endsWith(File.separator))
	  			localDPDirPathForThisAppId = localDPDirPath + appId.replace("_", "");
	  		else
	  			localDPDirPathForThisAppId = localDPDirPath + File.separator + appId.replace("_", "");
	  		
	  		log.debug("dataExecuteForDataUnit() localDPDirPathForThisAppId : {}", localDPDirPathForThisAppId);
	  		File fAppId = new File(localDPDirPathForThisAppId);
	  		if (false == fAppId.exists()) {
		  		if (false == fAppId.mkdir()) {
		  			// this is required if two DUs try to create directory at same time, one DU creates dir and returns true 
		  			// and the other DU fails to create dir and returns false
		  			if (false == fAppId.exists()) {
		  				throw new DScabiException("Error creating local directory for App Id " + localDPDirPathForThisAppId, "EXE.EFD.1");
		  			}
		  		}
		  	}

	  		String localDPDirPathForThisSplitRetryNum = null;
	  		String suffix = "_R" + retryNumber + "_P" + parallelNumber + "_S" + startCommandId + "_E" + endCommandId;
  			localDPDirPathForThisSplitRetryNum = localDPDirPathForThisAppId + File.separator + dj.getCU() + suffix;

  			/* cw
	  		if (localDPDirPath.endsWith(File.separator))
	  			localDPDirPathForThisSplitRetryNum = localDPDirPath + dj.getCU() + "_" + retryNumber;
	  		else
	  			localDPDirPathForThisSplitRetryNum = localDPDirPath + File.separator + dj.getCU() + "_" + retryNumber;
	  		*/
  			
	  		log.debug("dataExecuteForDataUnit() localDPDirPathForThisSplitRetryNum : {}", localDPDirPathForThisSplitRetryNum);
	  		File f = new File(localDPDirPathForThisSplitRetryNum);
	  		if (false == f.exists()) {
		  		if (false == f.mkdir())
		  			throw new DScabiException("Error creating local directory for split " + localDPDirPathForThisSplitRetryNum, "EXE.EFD.1");
	  		}
	  		
	  		// cw String appId = dj.getString("AppId");
			// cw log.debug("dataExecuteForDataUnit() AppId : {}", appId);
	  		
	  		String storageDPDirPath = null;
	  		IStorageHandler storageHandler = null;
	  		
			String storageProvider = ComputeServer_D2.getStorageProvider();
	  		// For storage system that can create directory
			// Pass the storage directory path through m_mountDir from ComputeServer_D2.getStorageDirPath() in case of DMHdfsStorageHandler
			if (storageProvider.equalsIgnoreCase("dfs") 
				|| storageProvider.equalsIgnoreCase("fuse")
				|| storageProvider.equalsIgnoreCase("nfs")) 
			{
	  			storageDPDirPath = ComputeServer_D2.getStorageDirPath();
	  			storageDPDirPath = getStorageAppIdDirPath(storageDPDirPath, appId);
	  			storageHandler = new DMStdStorageHandler();
			} else if (storageProvider.equalsIgnoreCase("seaweedfs")) {
				storageDPDirPath = appId; // For storage system that can not create directory, for example DMSeaweedStorageHandler
				String storageConfig = ComputeServer_D2.getStorageConfig();
				storageHandler = new DMSeaweedStorageHandler(storageConfig);
			} else {
				throw new DScabiException("Unknown StorageProvider : " + storageProvider, "EXE.EFD.1");
			}
		
			String configNodeType = djson.getString("ConfigNodeType");
			log.debug("dataExecuteForDataUnit() ConfigNodeType : {}", configNodeType);
			String splitAppId = dj.getCU() + "_" + appId.replace("_", "");
			log.debug("dataExecuteForDataUnit() splitAppId : {}", splitAppId);
			
			// Testing purpose dp = new DataPartition(dctx3, "mydata1", "mydata1_1", "/home/anees/testdata/bigfile/tutorial", "test_for_CU_" + dj.getCU(), 64 * 1024 * 1024);
			// Testing purpose String s = "testing string from CU" + dj.getCU();
			// Testing purpose dp.append(s);

			synchronized(ComputeServer_D2.m_splitAppIdIStorageHandlerMap) { ComputeServer_D2.m_splitAppIdIStorageHandlerMap.put(splitAppId, storageHandler); }
			
			if (isDataInitiatorProvided.equalsIgnoreCase("false")) {
				result = DMJson.ok();
				synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				return DMJson.ok();
			}
			
			String dataId = djson.getString("DataId");
			log.debug("dataExecuteForDataUnit() DataId : {}", dataId);
			String partitionId = dataId + "_" + dj.getCU() + "_" + appId.replace("_", "");
			log.debug("dataExecuteForDataUnit() PartitionId : {}", partitionId);
			
			if (retryNumber > 0) {
				/* cw
				log.debug("dataExecuteForDataUnit() retryNumber {} > 0. Proceeding with isPartitionExists() check", retryNumber);
				boolean check = DataPartition.isPartitionExists(appId, dataId, dj.getCU(), storageDPDirPath, storageHandler);
				
				if (check) {
					log.debug("dataExecuteForDataUnit() isPartitionExists() check. Partition already exists for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId, dj.getCU(), storageDPDirPath);					
					log.debug("dataExecuteForDataUnit() Deleting Partition for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId, dj.getCU(), storageDPDirPath);					
					DataPartition.deletePartition(appId, dataId, dj.getCU(), storageDPDirPath, storageHandler);
					log.debug("dataExecuteForDataUnit() Done Deleting Partition for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId, dj.getCU(), storageDPDirPath);					
				}
				*/
			}
			
			// cw dp = new DataPartition(dctx1, dataId, partitionId, storageDPDirPath, partitionId, 64 * 1024 * 1024, localDPDirPathForThisSplit, storageHandler);
			dp = DataPartition.allowCreateDataPartition(dctx1, dataId, partitionId, storageDPDirPath, partitionId, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, createdBy);			
			// cw synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { ComputeServer_D2.m_partitionIdDataPartitionMap.put(partitionId, dp); }
			synchronized(ComputeServer_D2.m_partitionIdRPSEDataPartitionMap) { ComputeServer_D2.m_partitionIdRPSEDataPartitionMap.put(partitionId + suffix, dp); }
			
		   	String hexStr = djson.getString("ClassBytes");
		   	// log.debug("dataExecuteForDataUnit() Hex string is : {}", hexStr);
		   	String className = djson.getString("ClassName");
		   	log.debug("dataExecuteForDataUnit() className is : {}", className);
		  	
		   	byte b2[] = DMUtil.toBytesFromHexStr(hexStr);

		   	// Not used ClassLoader cl = ClassLoader.getSystemClassLoader();
	  		boolean proceed = false;
	  		DataUnit cuu = null;
	  		try {
	  			Class<?> df = dcl.findClass(className, b2);
		  		cuu = (DataUnit) df.newInstance();
		  		proceed = true;
	  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
	  			// e.printStackTrace();
	  			proceed = false;
	  		} catch (ClassCastException e) {
	  			result = DMJson.error("CSE.EXE.DEFD.1", DMUtil.serverErrMsg(e));
	  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	  			return result;
	  		}
	  		
	  		log.debug("dataExecuteForDataUnit() TotalComputeUnit : {}", dj.getTU());
	  		log.debug("dataExecuteForDataUnit() SplitComputeUnit : {}", dj.getCU());
	  		log.debug("dataExecuteForDataUnit() JsonInput : {}", djson.getString("JsonInput"));
			
	  		if (proceed) {
	  			log.debug("dataExecuteForDataUnit() ComputeUnit cast is working ok for this object");
		  		try { 
		  			cuu.load(dp, dctx1);
		  			result = DMJson.ok();
		  		} catch(Throwable e) {
			   		result = DMJson.error("CSE.EXE.DEFD.2", DMUtil.serverErrMsg(e));
			   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   		return result;
			   	}
		  		log.debug("dataExecuteForDataUnit() loaded");
	  		} else {
	  			log.debug("dataExecuteForDataUnit() ComputeUnit cast is not working for this object. So proceeding with Class copy.");
	  			InputStream fis = new ByteArrayInputStream(b2);
		
				// Previous works ClassPool pool = ClassPool.getDefault();
				pool = new ClassPool(true);
				pool.appendSystemPath();
				// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
				pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
				// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
				CtClass cr = pool.makeClass(fis);
		  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
		  		log.debug("dataExecuteForDataUnit() modifiers : {}", cr.getModifiers());
		  		
		  		log.debug("dataExecuteForDataUnit() DMDataTemplate.class.getCanonicalName() : {}", DMDataTemplate.class.getCanonicalName());
		  		CtClass ct = pool.getAndRename(DMDataTemplate.class.getCanonicalName(), "DT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
		  		   
			    CtMethod amethods[] = cr.getDeclaredMethods();
			    for (CtMethod amethod : amethods) {
			    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
			    	
			    	// OR
			    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
			    	//bmethod.setBody(amethod, null);
			    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
			    	// OR
			    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
			    	
			    	ct.addMethod(bmethod);
			    }
			    
			    CtField afields[] = cr.getDeclaredFields();
			    for (CtField afield : afields) {
			    	CtField bfield = new CtField(afield, ct);
				    ct.addField(bfield);
			    }
			    
			    // Notes : Anonymous class can not define constructor. So no need to copy constructor
			    
		  		Class<?> df2 = ct.toClass();
		  		Object ob = df2.newInstance();
		  		Method m = df2.getMethod("load", DataPartition.class, DataContext.class);
		  		// OR try ((DataUnit) ob).load(dp, dctx3);
		  		try {
		  			m.invoke(ob, dp, dctx1);
		  			result = DMJson.ok();
		  		} catch(Throwable e) {
			  		cr.detach();
			 		ct.detach();
		  			pool = null;
			   		result = DMJson.error("CSE.EXE.DEFD.3", DMUtil.serverErrMsg(e));
			   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   		return result;
			   	}
		 		// Release CtClass from ClassPool
		  		cr.detach();
		 		ct.detach();
		 		pool = null;
		 		dcl = null;
		 		
		  		log.debug("dataExecuteForDataUnit() loaded");
	  		}
	   
		   //} // End For
	  		
	   	} catch (Error | RuntimeException e) {
  			pool = null;
  			result = DMJson.error("CSE.EXE.DEFD.4", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
	   	} catch(Exception e) {
	   		pool = null;
	   		result = DMJson.error("CSE.EXE.DEFD.5", DMUtil.serverErrMsg(e));
	   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	   		return result;
	   	} catch(Throwable e) {
	   		pool = null;
	   		result = DMJson.error("CSE.EXE.DEFD.6", DMUtil.serverErrMsg(e));
	   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	   		return result;
	   	}
		
	   	String status = null;
		synchronized(ComputeServer_D2.m_taskIdStatusMap) { status = ComputeServer_D2.m_taskIdStatusMap.get(taskId); }
		if (false == status.equals(ComputeServer_D2.S_EXECUTION_ERROR)) {
			String s = dp.prettyPrint();
			result = DMJson.result(s);
		}
		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
		// CW synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
	   
		return DMJson.ok();
	   		
	}
	
	public static String dataExecuteForOperators(DMClassLoader dcl, String request, DMJson dj) throws IOException {
		gc();
		
		String result = null;
		String taskId = null;
		DataPartition dp1 = null;
		DataPartition dp2 = null;
		
		// log.debug("dataExecuteForOperators() request : {}", request);
		try {
			taskId = dj.getString("TaskId");
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }

			long startCommandId = dj.getLongOf("StartCommandId");
			log.debug("dataExecuteForOperators() startCommandId : {}", startCommandId);
			
			long configCount = dj.getLongOf("ConfigCount");
			if (configCount < 2 && 1 == startCommandId) {
				log.debug("dataExecuteForOperators() configCount < 2 and startCommandId is 1. No operators.");
				return DMJson.ok();
			}

			long retryNumber = dj.getLongOf("RetryNumber");
			log.debug("dataExecuteForOperators() retryNumber : {}", retryNumber);
			long maxRetry = dj.getLongOf("MaxRetry");
			log.debug("dataExecuteForOperators() maxRetry : {}", maxRetry);
			String appId = dj.getString("AppId");
			log.debug("dataExecuteForOperators() AppId : {}", appId);
			long parallelNumber = dj.getLongOf("ParallelNumber");
			log.debug("dataExecuteForOperators() parallelNumber : {}", parallelNumber);
			long maxParallel = dj.getLongOf("MaxParallel");
			log.debug("dataExecuteForOperators() maxNumber : {}", maxParallel);
			long endCommandId = dj.getLongOf("EndCommandId");
			log.debug("dataExecuteForOperators() endCommandId : {}", endCommandId);
			
	  		DataContext dctx1 = new DataContext("TotalComputeUnit", dj.getString("TotalComputeUnit"));
	  		dctx1.add("SplitComputeUnit", dj.getString("SplitComputeUnit"));
	  		// cw dctx1.add("AppId", dj.getString("AppId"));
	  		dctx1.add("JobId", dj.getString("JobId"));
	    	dctx1.add("ConfigId", dj.getString("ConfigId"));
	    	dctx1.add("TaskId", dj.getString("TaskId"));
			dctx1.setRetryNumber(retryNumber);
			dctx1.setMaxRetry(maxRetry);
	    	dctx1.setParallelNumber(parallelNumber);
	    	dctx1.setMaxParallel(maxParallel);
	    	dctx1.setAppId(appId);
			
			DMJson djsonBy = new DMJson();
			djsonBy.add("AppId", dctx1.getAppId());
			djsonBy.add("SplitUnit", "" + dctx1.getDU());
			djsonBy.add("RetryNumber", "" + dctx1.getRetryNumber());
			djsonBy.add("ParallelNumber", "" + dctx1.getParallelNumber());
			String readBy = djsonBy.toString();
			String createdBy = readBy;
	    	
	  		String localDPDirPath = ComputeServer_D2.getLocalDirPath();
	  		String localDPDirPathForThisAppId = null;
			
	  		if (localDPDirPath.endsWith(File.separator))
	  			localDPDirPathForThisAppId = localDPDirPath + appId.replace("_", "");
	  		else
	  			localDPDirPathForThisAppId = localDPDirPath + File.separator + appId.replace("_", "");
	  		
	  		log.debug("dataExecuteForOperators() localDPDirPathForThisAppId : {}", localDPDirPathForThisAppId);
	  		File fAppId = new File(localDPDirPathForThisAppId);
	  		if (false == fAppId.exists()) {
		  		if (false == fAppId.mkdir()) {
		  			// this is required if two DUs try to create directory at same time, one DU creates dir and returns true 
		  			// and the other DU fails to create dir and returns false
		  			if (false == fAppId.exists()) {
		  				throw new DScabiException("Error creating local directory for App Id " + localDPDirPathForThisAppId, "EXE.EFD.1");
		  			}
		  		}
		  	}
	  		
			String localDPDirPathForThisSplitRetryNum = null;
	  		String suffix = "_R" + retryNumber + "_P" + parallelNumber + "_S" + startCommandId + "_E" + endCommandId;
  			localDPDirPathForThisSplitRetryNum = localDPDirPathForThisAppId + File.separator + dj.getCU() + suffix;
  			
			/* cw
	  		if (localDPDirPath.endsWith(File.separator))
	  			localDPDirPathForThisSplitRetryNum = localDPDirPath + dj.getCU() + "_" + retryNumber;
	  		else
	  			localDPDirPathForThisSplitRetryNum = localDPDirPath + File.separator + dj.getCU() + "_" + retryNumber;
	  		*/
			
	  		log.debug("dataExecuteForOperators() localDPDirPathForThisSplitRetryNum : {}", localDPDirPathForThisSplitRetryNum);
	  		File f = new File(localDPDirPathForThisSplitRetryNum);
	  		if (false == f.exists()) {
		  		if (false == f.mkdir())
		  			throw new DScabiException("Error creating local directory for split " + localDPDirPathForThisSplitRetryNum, "EXE.EFO.1");
	  		}
	  		
			// cw String appId = dj.getString("AppId");
			// cw log.debug("AppId : {}", appId);
	  		
			String storageDPDirPath = null;
					
			String storageProvider = ComputeServer_D2.getStorageProvider();
	  		// For storage system that can create directory
			// Pass the storage directory path through m_mountDir from ComputeServer_D2.getStorageDirPath() in case of DMHdfsStorageSystem
			if (storageProvider.equalsIgnoreCase("dfs") 
				|| storageProvider.equalsIgnoreCase("fuse")
				|| storageProvider.equalsIgnoreCase("nfs")) {
	  			storageDPDirPath = ComputeServer_D2.getStorageDirPath();
	  			storageDPDirPath = getStorageAppIdDirPath(storageDPDirPath, appId);	
			}
			else {
				storageDPDirPath = appId; // For storage system that can not create directory, for example DMSeaweedStorageHandler
			}
			// cw long endCommandId = dj.getLongOf("EndCommandId");
			// cw log.debug("dataExecuteForOperators() endCommandId : {}", endCommandId);
			// cw int retryNumber = dj.getIntOf("RetryNumber");
			// cw log.debug("dataExecuteForOperators() retryNumber : {}", retryNumber);
			String splitAppId = dj.getCU() + "_" + appId.replace("_", "");
			log.debug("dataExecuteForOperators() splitAppId : {}", splitAppId);

			long i = 1;
			if (1 == startCommandId)
				i = 2;
			
		   	for (/*cw long i = 2*/; i <= configCount; i++) {

			String jsonStr = dj.getString("" + i);
		   	DMJson djson = new DMJson(jsonStr);
		   
			String commandId = djson.getString("CommandId");
			log.debug("dataExecuteForOperators() CommandId : {}", commandId);
			
			long cmdId = Long.parseLong(commandId);
			if (cmdId < startCommandId || cmdId > endCommandId) {
				continue;
			}
				
	    	dctx1.add("JsonInput", djson.getString("JsonInput"));
			log.debug("dataExecuteForOperators() JsonInput : {}", djson.getString("JsonInput"));
			String configNodeType = djson.getString("ConfigNodeType");
			log.debug("dataExecuteForOperators() ConfigNodeType : {}", configNodeType);
			String dataId1 = djson.getString("SourceDataId");
			log.debug("dataExecuteForOperators() DataId1 : {}", dataId1);
			String dataId2 = djson.getString("TargetDataId");
			log.debug("dataExecuteForOperators() DataId2 : {}", dataId2);
			String partitionId1 = dataId1 + "_" + dj.getCU() + "_" + appId.replace("_", "");
			log.debug("dataExecuteForOperators() PartitionId1 : {}", partitionId1);
			String partitionId2 = dataId2 + "_" + dj.getCU() + "_" + appId.replace("_", "");
			log.debug("dataExecuteForOperators() PartitionId2 : {}", partitionId2);
			
		   	// cw synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { dp1 = ComputeServer_D2.m_partitionIdDataPartitionMap.get(partitionId1); }
		   	synchronized(ComputeServer_D2.m_partitionIdRPSEDataPartitionMap) { dp1 = ComputeServer_D2.m_partitionIdRPSEDataPartitionMap.get(partitionId1 + suffix); }
		   	
			IStorageHandler storageHandler = null;
			synchronized(ComputeServer_D2.m_splitAppIdIStorageHandlerMap) { storageHandler = ComputeServer_D2.m_splitAppIdIStorageHandlerMap.get(splitAppId); }			
			
			if (retryNumber > 0) {
				
			   	if (null == storageHandler) {
					if (storageProvider.equalsIgnoreCase("dfs") 
						|| storageProvider.equalsIgnoreCase("fuse")
						|| storageProvider.equalsIgnoreCase("nfs")) 
					{
				  			storageHandler = new DMStdStorageHandler();
					} else if (storageProvider.equalsIgnoreCase("seaweedfs")) {
							String storageConfig = ComputeServer_D2.getStorageConfig();
							storageHandler = new DMSeaweedStorageHandler(storageConfig);
					} else {
							throw new DScabiException("Unknown StorageProvider : " + storageProvider, "EXE.EFD.1");
					}
					synchronized(ComputeServer_D2.m_splitAppIdIStorageHandlerMap) { ComputeServer_D2.m_splitAppIdIStorageHandlerMap.put(splitAppId, storageHandler); }
			   	}

			   	if (null == dp1) {
			   		log.debug("dataExecuteForOperators() Reconstruct source data partition - dp1 is null");
			   		/* cw
			   		log.debug("dataExecuteForOperators() Reconstruct source data partition - dp1 is null. Proceeding with isPartitionExists() check");			   		
			   		boolean check = DataPartition.isPartitionExists(appId, dataId1, dj.getCU(), storageDPDirPath, storageHandler);
					
					if (check) {
						log.debug("dataExecuteForOperators() Reconstruct source data partition - isPartitionExists() check. Partition already exists for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId1, dj.getCU(), storageDPDirPath);
						dp1 = new DataPartition(dctx1, dataId1, partitionId1, storageDPDirPath, partitionId1, 64 * 1024 * 1024, localDPDirPathForThisSplit, storageHandler);
						synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { ComputeServer_D2.m_partitionIdDataPartitionMap.put(partitionId1, dp1); }
					} else {
					   	throw new DScabiException("Source DataPartition dataID : " + dataId1 + " partitionId : " + partitionId1 + " is not found in Storage system", "EXE.EFO.1");
					}
					*/
			   		/* cw
			  		File f = new File(localDPDirPathForThisSplit);
			  		if (false == f.exists()) {
				  		if (false == f.mkdir())
				  			throw new DScabiException("Error creating local directory for split " + localDPDirPathForThisSplit, "EXE.EFD.1");
			  		}
			  		*/
					dp1 = DataPartition.readDataPartition(dctx1, dataId1, partitionId1, storageDPDirPath, partitionId1, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, readBy);
					// cw synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { ComputeServer_D2.m_partitionIdDataPartitionMap.put(partitionId1, dp1); }	
					synchronized(ComputeServer_D2.m_partitionIdRPSEDataPartitionMap) { ComputeServer_D2.m_partitionIdRPSEDataPartitionMap.put(partitionId1 + suffix, dp1); }	
			   	}

			   	/* cw
				log.debug("dataExecuteForOperators() retryNumber {} > 0. Proceeding with isPartitionExists() check", retryNumber);
				boolean check2 = DataPartition.isPartitionExists(appId, dataId2, dj.getCU(), storageDPDirPath, storageHandler);
				
				if (check2) {
					log.debug("dataExecuteForOperators() isPartitionExists() check. Partition already exists for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId2, dj.getCU(), storageDPDirPath);					
					log.debug("dataExecuteForOperators() Deleting Partition for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId2, dj.getCU(), storageDPDirPath);					
					DataPartition.deletePartition(appId, dataId2, dj.getCU(), storageDPDirPath, storageHandler);
					log.debug("dataExecuteForOperators() Done Deleting Partition for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId2, dj.getCU(), storageDPDirPath);					
				}
			   	*/
			} else {
			   	if (null == storageHandler)
			   		throw new DScabiException("splitAppId : " + splitAppId + " is not found in ComputeServer_D2.m_splitAppIdIStorageHandlerMap", "EXE.EFO.2");	
			   	
			   	if (null == dp1) {
			   		// throw new DScabiException("Source DataPartition dataID : " + dataId1 + " partitionId : " + partitionId1 + " is not found in ComputeServer_D2.m_partitionIdDataPartitionMap", "EXE.EFO.1");
			   		log.debug("dataExecuteForOperators() Else part - Reconstruct source data partition - dp1 is null");
			   		/* cw
			   		log.debug("dataExecuteForOperators() Else part - Reconstruct source data partition - dp1 is null. Proceeding with isPartitionExists() check");			   		
			   		boolean check3 = DataPartition.isPartitionExists(appId, dataId1, dj.getCU(), storageDPDirPath, storageHandler);
					
					if (check3) {
						log.debug("dataExecuteForOperators() Else part - Reconstruct source data partition - isPartitionExists() check. Partition already exists for appId : {}, dataId : {}, dj.getCU() : {}, storageDPDirPath : {}", appId, dataId1, dj.getCU(), storageDPDirPath);
						dp1 = new DataPartition(dctx1, dataId1, partitionId1, storageDPDirPath, partitionId1, 64 * 1024 * 1024, localDPDirPathForThisSplit, storageHandler);
						synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { ComputeServer_D2.m_partitionIdDataPartitionMap.put(partitionId1, dp1); }
					} else {
					   	throw new DScabiException("Source DataPartition dataID : " + dataId1 + " partitionId : " + partitionId1 + " is not found in Storage system", "EXE.EFO.1");
					}	
					*/
					dp1 = DataPartition.readDataPartition(dctx1, dataId1, partitionId1, storageDPDirPath, partitionId1, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, readBy);
					// cw synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { ComputeServer_D2.m_partitionIdDataPartitionMap.put(partitionId1, dp1); }
					synchronized(ComputeServer_D2.m_partitionIdRPSEDataPartitionMap) { ComputeServer_D2.m_partitionIdRPSEDataPartitionMap.put(partitionId1 + suffix, dp1); }
			   	}	
			}
			
		   	// cw dp2 = new DataPartition(dctx1, dataId2, partitionId2, storageDPDirPath, partitionId2, 64 * 1024 * 1024, localDPDirPathForThisSplit, storageHandler);
		   	dp2 = DataPartition.allowCreateDataPartition(dctx1, dataId2, partitionId2, storageDPDirPath, partitionId2, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, createdBy);			
		   	// cw synchronized(ComputeServer_D2.m_partitionIdDataPartitionMap) { ComputeServer_D2.m_partitionIdDataPartitionMap.put(partitionId2, dp2); }
		   	synchronized(ComputeServer_D2.m_partitionIdRPSEDataPartitionMap) { ComputeServer_D2.m_partitionIdRPSEDataPartitionMap.put(partitionId2 + suffix, dp2); }
		   			   	
		   	String hexStr = null;
		   	String className = null;
		   	byte b2[] = null;
		   	if (Integer.parseInt(configNodeType) != DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_2_1) {
			   	hexStr = djson.getString("ClassBytes");
			   	// log.debug("dataExecuteForOperators() Hex string is : {}", hexStr);
			   	className = djson.getString("ClassName");
			   	log.debug("dataExecuteForOperators() className is : {}", className);
			  	
			   	b2 = DMUtil.toBytesFromHexStr(hexStr);
		   	}
		   	// Not used ClassLoader cl = ClassLoader.getSystemClassLoader();
		   
		   	// Big if
		   	if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1) {
		   		result = dataExecuteForOperator_1_1(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId);
		   	} else if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2) {
		   		result = dataExecuteForOperator_1_2(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId);
		   	} else if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_1) {
		   		result = dataExecuteForShuffle_1_1(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId, storageDPDirPath, localDPDirPathForThisSplitRetryNum, storageHandler);
		   	} else if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_2) {
		   		result = dataExecuteForShuffle_1_2(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId, storageDPDirPath, localDPDirPathForThisSplitRetryNum, storageHandler);
		   	} else if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_2_1) {
		   		result = dataExecuteForShuffle_2_1(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId, storageDPDirPath, localDPDirPathForThisSplitRetryNum, storageHandler);
		   	} else if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_COMPARATOR_CONFIG_1_1) {
		   		result = dataExecuteForComparator_1_1(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId);
		   	} 
		   	/* later
		   	else if (Integer.parseInt(configNodeType) == DataAsyncConfigNode.CNT_COMPARATOR_CONFIG_1_2) {
		   		result = dataExecuteForComparator_1_2(dcl, dj, djson, className, b2, dctx1, dp1, dp2, taskId);
		   	}
		   	*/
		   	// End Big if
		   	
		   	} // End For
	  		
		   	String status = null;
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { status = ComputeServer_D2.m_taskIdStatusMap.get(taskId); }
			if (false == status.equals(ComputeServer_D2.S_EXECUTION_ERROR)) {
			   	String s = dp2.prettyPrint();
			   	result = DMJson.result(s);	
			}
		   	
	   	} catch (Error | RuntimeException e) {
  			result = DMJson.error("CSE.EXE.DEFO.1", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
	   	} catch(Exception e) {
	   		result = DMJson.error("CSE.EXE.DEFO.2", DMUtil.serverErrMsg(e));
	   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	   		return result;
	   	} catch(Throwable e) {
	   		result = DMJson.error("CSE.EXE.DEFO.3", DMUtil.serverErrMsg(e));
	   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	   		return result;
	   	}
		
		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }

		return DMJson.ok();	
	}	
	
	public static String dataExecuteForOperator_1_1(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException {
		
		ClassPool pool = null;
		String result = null;
  		boolean proceed = false;
  		IOperator cuu = null;
  		try {
  			Class<?> df = dcl.findClass(className, b2);
	  		cuu = (IOperator) df.newInstance();
	  		proceed = true;
  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
  			// e.printStackTrace();
  			proceed = false;
  		} catch (ClassCastException e) {
  			result = DMJson.error("CSE.EXE.DEFO2.1", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
  		}
  		
  		log.debug("dataExecuteForOperator_1_1() TotalComputeUnit : {}", dj.getTU());
  		log.debug("dataExecuteForOperator_1_1() SplitComputeUnit : {}", dj.getCU());
  		log.debug("dataExecuteForOperator_1_1() JsonInput : {}", djson.getString("JsonInput"));
		
  		if (proceed) {
  			log.debug("dataExecuteForOperator_1_1() ComputeUnit cast is working ok for this object");
	  		try { 
	  			// TODO set a read only attribute to dp1.setReadOnly()
	  			cuu.operate(dp1, dp2, dctx1);
	  		} catch(Throwable e) {
		   		result = DMJson.error("CSE.EXE.DEFO2.2", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	}
	  		log.debug("dataExecuteForOperator_1_1() operation done");
  		} else {
  			log.debug("dataExecuteForOperator_1_1() IOperator cast is not working for this object. So proceeding with Class copy.");
  			InputStream fis = new ByteArrayInputStream(b2);
	
			// Previous works ClassPool pool = ClassPool.getDefault();
			pool = new ClassPool(true);
			pool.appendSystemPath();
			// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
			pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
			// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
			CtClass cr = pool.makeClass(fis);
	  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
	  		log.debug("dataExecuteForOperator_1_1() modifiers : {}", cr.getModifiers());
	  		
	  		log.debug("dataExecuteForOperator_1_1() DMOperatorTemplate.class.getCanonicalName() : {}", DMOperatorTemplate.class.getCanonicalName());
	  		CtClass ct = pool.getAndRename(DMOperatorTemplate.class.getCanonicalName(), "OT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
		    CtMethod ctmethods[] = ct.getDeclaredMethods();
		    for (CtMethod ctmethod : ctmethods)
		  		   ct.removeMethod(ctmethod);
	  		   
		    CtMethod amethods[] = cr.getDeclaredMethods();
		    for (CtMethod amethod : amethods) {
		    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
		    	
		    	// OR
		    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
		    	//bmethod.setBody(amethod, null);
		    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
		    	// OR
		    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
		    	
		    	ct.addMethod(bmethod);
		    }
		    
		    CtField afields[] = cr.getDeclaredFields();
		    for (CtField afield : afields) {
		    	CtField bfield = new CtField(afield, ct);
			    ct.addField(bfield);
		    }
		    
		    // Notes : Anonymous class can not define constructor. So no need to copy constructor
		    
	  		Class<?> df2 = ct.toClass();
	  		Object ob = df2.newInstance();
	  		Method m = df2.getMethod("operate", DataPartition.class, DataPartition.class, DataContext.class);
	  		/* OR try
	  		IOperator  iob = (IOperator) ob;
	  		iob.operate(dp1, dp2, dctx3);
	  		*/
	  		try {
	  			// TODO set a read only attribute to dp1.setReadOnly()
	  			m.invoke(ob, dp1, dp2, dctx1);
	  		} catch(Throwable e) {
		  		cr.detach();
		 		ct.detach();
	  			pool = null;
		   		result = DMJson.error("CSE.EXE.DEFO2.3", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	}
	 		// Release CtClass from ClassPool
	  		cr.detach();
	 		ct.detach();
	 		pool = null;
	 		
	  		log.debug("dataExecuteForOperator_1_1() operation done");
  		}		
  		return DMJson.ok();	
	}
	
	public static String dataExecuteForOperator_1_2(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException {

		ClassPool pool = null;
		String result = null;
  		boolean proceed = false;
  		IOperator cuu = null;
  		
  		log.debug("dataExecuteForOperator_1_2() TotalComputeUnit : {}", dj.getTU());
  		log.debug("dataExecuteForOperator_1_2() SplitComputeUnit : {}", dj.getCU());
  		log.debug("dataExecuteForOperator_1_2() JsonInput : {}", djson.getString("JsonInput"));
  		
  		InputStream fis = new ByteArrayInputStream(b2);
	
  		// Previous works ClassPool pool = ClassPool.getDefault();
  		pool = new ClassPool(true);
  		pool.appendSystemPath();
  		// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
  		pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
  		// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
  		CtClass cr = pool.makeClass(fis);
  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
  		log.debug("dataExecuteForOperator_1_2() modifiers : {}", cr.getModifiers());
	  		
  		log.debug("dataExecuteForOperator_1_2() DMDataTemplate.class.getCanonicalName() : {}", DMDataTemplate.class.getCanonicalName());
  		CtClass ct = pool.getAndRename(DMDataTemplate.class.getCanonicalName(), "OT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
  		// Not used CtMethod ctmethods[] = ct.getDeclaredMethods();
  		// Not used for (CtMethod ctmethod : ctmethods)
  			// Not used ct.removeMethod(ctmethod);
  		String lambdaMethodName = djson.getString("LambdaMethodName");   
  		log.debug("dataExecuteForOperator_1_2() lambdaMethodName: {}", lambdaMethodName);
  		CtMethod amethods[] = cr.getDeclaredMethods();
  		for (CtMethod amethod : amethods) {
  			if (amethod.getName().equals(lambdaMethodName)) {
  				log.debug("dataExecuteForOperator_1_2() Match found for lambdaMethodName: {}", lambdaMethodName);
  				CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
		    	
  				// OR
		    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
		    	//bmethod.setBody(amethod, null);
		    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
		    	// OR
		    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
		    		
  				ct.addMethod(bmethod);
  			}
  		}
		    
  		//CtField afields[] = cr.getDeclaredFields();
  		//for (CtField afield : afields) {
  		//	CtField bfield = new CtField(afield, ct);
  		//    ct.addField(bfield);
  		//}
		    
  		// Notes : Anonymous class can not define constructor. So no need to copy constructor
		    
  		Class<?> df2 = ct.toClass();
  		// Object ob = df2.newInstance();
  		// Method m = df2.getMethod("operate", DataPartition.class, DataPartition.class, DataContext.class);
  		Method m = df2.getDeclaredMethod(lambdaMethodName, DataPartition.class, DataPartition.class, DataContext.class);
  		m.setAccessible(true);
  		/* OR try
	  		IOperator  iob = (IOperator) ob;
	  		iob.operate(dp1, dp2, dctx3);
  		 */
  		try {
  			// TODO set a read only attribute to dp1.setReadOnly()
  			m.invoke(null, dp1, dp2, dctx1);
  		} catch(Throwable e) {
  			cr.detach();
  			ct.detach();
  			pool = null;
  			result = DMJson.error("CSE.EXE.DEFO3.1", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
  			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
  		}
  		// Release CtClass from ClassPool
  		cr.detach();
  		ct.detach();
  		pool = null;
	 		
  		log.debug("dataExecuteForOperator_1_2() operation done");
  		
  		return DMJson.ok();	
		
	}
	
	public static String dataExecuteForShuffle_1_1(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId, String storageDPDirPath, String localDPDirPathForThisSplitRetryNum, IStorageHandler storageHandler) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException, DScabiException {

  		log.debug("dataExecuteForShuffle_1_1() INSIDE dataExecuteForShuffle_1_1");
  		log.debug("dataExecuteForShuffle_1_1() INSIDE dataExecuteForShuffle_1_1");
  		log.debug("dataExecuteForShuffle_1_1() INSIDE dataExecuteForShuffle_1_1");
  		log.debug("dataExecuteForShuffle_1_1() INSIDE dataExecuteForShuffle_1_1");
  		log.debug("dataExecuteForShuffle_1_1() INSIDE dataExecuteForShuffle_1_1");
		
		DMJson djsonBy = new DMJson();
		djsonBy.add("AppId", dctx1.getAppId());
		djsonBy.add("SplitUnit", "" + dctx1.getDU());
		djsonBy.add("RetryNumber", "" + dctx1.getRetryNumber());
		djsonBy.add("ParallelNumber", "" + dctx1.getParallelNumber());
		String readBy = djsonBy.toString();
  		
		ClassPool pool = null;
		String result = null;
  		boolean proceed = false;
  		IShuffle cuu = null;
  		try {
  			Class<?> df = dcl.findClass(className, b2);
	  		cuu = (IShuffle) df.newInstance();
	  		proceed = true;
  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
  			// e.printStackTrace();
  			proceed = false;
  		} catch (ClassCastException e) {
  			result = DMJson.error("CSE.EXE.DEFS.1", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
  		}
  		
  		log.debug("dataExecuteForShuffle_1_1() TotalComputeUnit : {}", dj.getTU());
  		log.debug("dataExecuteForShuffle_1_1() SplitComputeUnit : {}", dj.getCU());
  		log.debug("dataExecuteForShuffle_1_1() JsonInput : {}", djson.getString("JsonInput"));
		
		long tu = dj.getTU();
		long cu = dj.getCU();
		
		long retryNumber = dj.getLongOf("RetryNumber");
		log.debug("dataExecuteForShuffle_1_1() retryNumber : {}", retryNumber);

		/* cw
		String localDPDirPathForThisSplitRetryNum = null;
		
  		if (localDPDirPath.endsWith(File.separator))
  			localDPDirPathForThisSplitRetryNum = localDPDirPath + dj.getCU() + "_" + retryNumber;
  		else
  			localDPDirPathForThisSplitRetryNum = localDPDirPath + File.separator + dj.getCU() + "_" + retryNumber;
  		
  		log.debug("dataExecuteForShuffle_1_1() localDPDirPathForThisSplitRetryNum : {}", localDPDirPathForThisSplitRetryNum);
		*/
		
		String appId = dj.getString("AppId");
		log.debug("dataExecuteForShuffle_1_1() AppId : {}", appId);
		String dataId1 = djson.getString("SourceDataId");
		log.debug("dataExecuteForShuffle_1_1() DataId1 : {}", dataId1);
		
  		if (proceed) {
  			log.debug("dataExecuteForShuffle_1_1() ComputeUnit cast is working ok for this object");
	  		try { 
	  			// TODO set a read only attribute to dp1.setReadOnly()

	  			// create DataPartition object for other source dp with partition id dataid1_i_appid of source dataset dataId1
	  			for (int i = 1; i <= tu; i++) {
	  				
	  				if (i == cu) {
	  					// use dp1
	  		  			dp1.shuffleByValues(cuu);
	  					// add filtered entries from dp1 to dp2
	  		  			dp1.begin();
	  		  			while (dp1.hasNext()) {
	  		  				DataElement e = dp1.next();
	  		  				if (dp1.isCurrentElementBelongsToSU(tu, cu)) {
	  		  					dp2.append(e);
	  		  				}
	  		  			}
	  		  			dp1.clearShuffle();
	  				} else {
	  					// check if other source dp exists and create DataPartition object if other source dp exists
	  					String partitionIdOther = dataId1 + "_" + i + "_" + appId.replace("_", "");
	  					log.debug("dataExecuteForShuffle_1_1() PartitionIdOther : {}", partitionIdOther);
	  					DataPartition dpSourceOther = null;
	  					/* cw
				   		log.debug("dataExecuteForShuffle_1_1() Proceeding with isPartitionExists() check for partition id : {}", partitionIdOther);			   		
				   		boolean check = DataPartition.isPartitionExists(appId, dataId1, i, storageDPDirPath, storageHandler);
						
				   		DataPartition dpSourceOther = null;
						if (check) {
							log.debug("dataExecuteForShuffle_1_1() Partition exists for appId : {}, dataId : {}, i : {}, storageDPDirPath : {}", appId, dataId1, i, storageDPDirPath);
							dpSourceOther = new DataPartition(dctx1, dataId1, partitionIdOther, storageDPDirPath, partitionIdOther, 64 * 1024 * 1024, localDPDirPathForThisSplit, storageHandler);
						} else {
						   	throw new DScabiException("Source DataPartition for partitionId : " + partitionIdOther + " is not found in Storage system", "EXE.EFS.1");
						}
						*/
						dpSourceOther = DataPartition.readDataPartition(dctx1, dataId1, partitionIdOther, storageDPDirPath, partitionIdOther, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, readBy);						
						
						dpSourceOther.shuffleByValues(cuu);
	  					// add filtered entries from dpSourceOther to dp2
						dpSourceOther.begin();
	  		  			while (dpSourceOther.hasNext()) {
	  		  				DataElement e = dpSourceOther.next();
	  		  				if (dpSourceOther.isCurrentElementBelongsToSU(tu, cu)) {
	  		  					dp2.append(e);
	  		  				}
	  		  			}
	  		  			dpSourceOther.close();
	  		  			dpSourceOther = null;
	  				}
	  			}
 			
	  		} catch(Throwable e) {
		   		result = DMJson.error("CSE.EXE.DEFS.2", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	}
	  		log.debug("dataExecuteForShuffle_1_1() operation done");
  		} else {
  			log.debug("dataExecuteForShuffle_1_1() IOperator cast is not working for this object. So proceeding with Class copy.");
  			InputStream fis = new ByteArrayInputStream(b2);
	
			// Previous works ClassPool pool = ClassPool.getDefault();
			pool = new ClassPool(true);
			pool.appendSystemPath();
			// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
			pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
			// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
			CtClass cr = pool.makeClass(fis);
	  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
	  		log.debug("dataExecuteForShuffle_1_1() modifiers : {}", cr.getModifiers());
	  		
	  		log.debug("dataExecuteForShuffle_1_1() DMOperatorTemplate.class.getCanonicalName() : {}", DMOperatorTemplate.class.getCanonicalName());
	  		CtClass ct = pool.getAndRename(DMOperatorTemplate.class.getCanonicalName(), "OT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
		    CtMethod ctmethods[] = ct.getDeclaredMethods();
		    for (CtMethod ctmethod : ctmethods)
		  		   ct.removeMethod(ctmethod);
	  		   
		    CtMethod amethods[] = cr.getDeclaredMethods();
		    for (CtMethod amethod : amethods) {
		    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
		    	
		    	// OR
		    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
		    	//bmethod.setBody(amethod, null);
		    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
		    	// OR
		    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
		    	
		    	ct.addMethod(bmethod);
		    }
		    
		    CtField afields[] = cr.getDeclaredFields();
		    for (CtField afield : afields) {
		    	CtField bfield = new CtField(afield, ct);
			    ct.addField(bfield);
		    }
		    
		    // Notes : Anonymous class can not define constructor. So no need to copy constructor
		    
	  		Class<?> df2 = ct.toClass();
	  		Object ob = df2.newInstance();
	  		Method m = df2.getMethod("groupByValues", DataElement.class, DataContext.class);
	  		
	  		DMShuffle shuffleObj = new DMShuffle(m, ob);
  		
	  		/* OR try
	  		IOperator  iob = (IOperator) ob;
	  		iob.operate(dp1, dp2, dctx3);
	  		*/
	  		try {
	  			// TODO set a read only attribute to dp1.setReadOnly()
	  		
	  			// create DataPartition object for other source dp with partition id dataid1_i_appid of source dataset dataId1
	  			for (int i = 1; i <= tu; i++) {
	  				
	  				if (i == cu) {
	  					// use dp1
	  		  			dp1.shuffleByValues(shuffleObj);
	  					// add filtered entries from dp1 to dp2
	  		  			dp1.begin();
	  		  			while (dp1.hasNext()) {
	  		  				DataElement e = dp1.next();
	  		  				if (dp1.isCurrentElementBelongsToSU(tu, cu)) {
	  		  					dp2.append(e);
	  		  				}
	  		  			}
	  		  			dp1.clearShuffle();
	  				} else {
	  					// check if other source dp exists and create DataPartition object if other source dp exists
	  					String partitionIdOther = dataId1 + "_" + i + "_" + appId.replace("_", "");
	  					log.debug("dataExecuteForShuffle_1_1() PartitionId1 : {}", partitionIdOther);
	  					DataPartition dpSourceOther = null;
	  					/* cw
				   		log.debug("dataExecuteForShuffle_1_1() Proceeding with isPartitionExists() check for partition id : {}", partitionIdOther);			   		
				   		boolean check = DataPartition.isPartitionExists(appId, dataId1, i, storageDPDirPath, storageHandler);
						
				   		DataPartition dpSourceOther = null;
						if (check) {
							log.debug("dataExecuteForShuffle_1_1() Partition exists for appId : {}, dataId : {}, i : {}, storageDPDirPath : {}", appId, dataId1, i, storageDPDirPath);
							dpSourceOther = new DataPartition(dctx1, dataId1, partitionIdOther, storageDPDirPath, partitionIdOther, 64 * 1024 * 1024, localDPDirPathForThisSplit, storageHandler);
						} else {
						   	throw new DScabiException("Source DataPartition for partitionId : " + partitionIdOther + " is not found in Storage system", "EXE.EFS.1");
						}
						*/
						dpSourceOther = DataPartition.readDataPartition(dctx1, dataId1, partitionIdOther, storageDPDirPath, partitionIdOther, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, readBy);  					
						dpSourceOther.shuffleByValues(shuffleObj);
	  					// add filtered entries from dpSourceOther to dp2
						dpSourceOther.begin();
	  		  			while (dpSourceOther.hasNext()) {
	  		  				DataElement e = dpSourceOther.next();
	  		  				if (dpSourceOther.isCurrentElementBelongsToSU(tu, cu)) {
	  		  					dp2.append(e);
	  		  				}
	  		  			}
	  		  			dpSourceOther.close();
	  		  			dpSourceOther = null;
	  				}
	  			}
	  		
	  		} catch(Throwable e) {
		  		cr.detach();
		 		ct.detach();
	  			pool = null;
	  			fis.close();
		   		result = DMJson.error("CSE.EXE.DEFO2.3", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	}
	 		// Release CtClass from ClassPool
	  		cr.detach();
	 		ct.detach();
	 		pool = null;
	 		fis.close();
	  		log.debug("dataExecuteForShuffle_1_1() operation done");
  		}	
		return DMJson.ok();
	}

	public static String dataExecuteForShuffle_1_2(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId, String storageDPDirPath, String localDPDirPathForThisSplitRetryNum, IStorageHandler storageHandler) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException, DScabiException {
	
		DMJson djsonBy = new DMJson();
		djsonBy.add("AppId", dctx1.getAppId());
		djsonBy.add("SplitUnit", "" + dctx1.getDU());
		djsonBy.add("RetryNumber", "" + dctx1.getRetryNumber());
		djsonBy.add("ParallelNumber", "" + dctx1.getParallelNumber());
		String readBy = djsonBy.toString();
		
  		log.debug("dataExecuteForShuffle_1_2() TotalComputeUnit : {}", dj.getTU());
  		log.debug("dataExecuteForShuffle_1_2() SplitComputeUnit : {}", dj.getCU());
  		log.debug("dataExecuteForShuffle_1_2() JsonInput : {}", djson.getString("JsonInput"));
		
		long tu = dj.getTU();
		long cu = dj.getCU();
		
		ClassPool pool = null;
		String result = null;
  		boolean proceed = false;
  		IOperator cuu = null;
  		
  		log.debug("dataExecuteForShuffle_1_2() TotalComputeUnit : {}", dj.getTU());
  		log.debug("dataExecuteForShuffle_1_2() SplitComputeUnit : {}", dj.getCU());
  		log.debug("dataExecuteForShuffle_1_2() JsonInput : {}", djson.getString("JsonInput"));
  		
  		InputStream fis = new ByteArrayInputStream(b2);
	
  		// Previous works ClassPool pool = ClassPool.getDefault();
  		pool = new ClassPool(true);
  		pool.appendSystemPath();
  		// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
  		pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
  		// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
  		CtClass cr = pool.makeClass(fis);
  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
  		log.debug("dataExecuteForShuffle_1_2() modifiers : {}", cr.getModifiers());
	  		
  		log.debug("dataExecuteForShuffle_1_2() DMDataTemplate.class.getCanonicalName() : {}", DMDataTemplate.class.getCanonicalName());
  		CtClass ct = pool.getAndRename(DMDataTemplate.class.getCanonicalName(), "OT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());

  		String lambdaMethodName = djson.getString("LambdaMethodName");   
  		log.debug("dataExecuteForShuffle_1_2() lambdaMethodName: {}", lambdaMethodName);
  		CtMethod amethods[] = cr.getDeclaredMethods();
  		for (CtMethod amethod : amethods) {
  			if (amethod.getName().equals(lambdaMethodName)) {
  				log.debug("dataExecuteForShuffle_1_2() Match found for lambdaMethodName: {}", lambdaMethodName);
  				CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
		    	
  				// OR
		    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
		    	//bmethod.setBody(amethod, null);
		    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
		    	// OR
		    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
		    		
  				ct.addMethod(bmethod);
  			}
  		}
		    
  		//CtField afields[] = cr.getDeclaredFields();
  		//for (CtField afield : afields) {
  		//	CtField bfield = new CtField(afield, ct);
  		//    ct.addField(bfield);
  		//}
		    
  		// Notes : Anonymous class can not define constructor. So no need to copy constructor
		    
  		Class<?> df2 = ct.toClass();
  		// Object ob = df2.newInstance();
  		// Method m = df2.getMethod("groupByValues", DataElement.class, DataContext.class);
  		Method m = df2.getDeclaredMethod(lambdaMethodName, DataElement.class, DataContext.class);
  		m.setAccessible(true);
  		/* OR try
	  		IShuffle  iob = (IShuffle) ob; // Possible only if ob implements IShuffle
	  		iob.groupByValues(de1, dctx3);
  		 */
  		
		String appId = dj.getString("AppId");
		log.debug("dataExecuteForShuffle_1_2() AppId : {}", appId);
		String dataId1 = djson.getString("SourceDataId");
		log.debug("dataExecuteForShuffle_1_2() DataId1 : {}", dataId1);
		
  		try {
  			DMShuffle dmshuffle = new DMShuffle(m, null);
  			// TODO set a read only attribute to dp1.setReadOnly()
  			// create DataPartition object for other source dp with partition id dataid1_i_appid of source dataset dataId1
  			for (int i = 1; i <= tu; i++) {
  				
  				if (i == cu) {
  					// use dp1
  		  			dp1.shuffleByValues(dmshuffle);
  					// add filtered entries from dp1 to dp2
  		  			dp1.begin();
  		  			while (dp1.hasNext()) {
  		  				DataElement e = dp1.next();
  		  				if (dp1.isCurrentElementBelongsToSU(tu, cu)) {
  		  					dp2.append(e);
  		  				}
  		  			}
  		  			dp1.clearShuffle();
  				} else {
  					// check if other source dp exists and create DataPartition object if other source dp exists
  					String partitionIdOther = dataId1 + "_" + i + "_" + appId.replace("_", "");
  					log.debug("dataExecuteForShuffle_1_2() PartitionIdOther : {}", partitionIdOther);
  					DataPartition dpSourceOther = null;

					dpSourceOther = DataPartition.readDataPartition(dctx1, dataId1, partitionIdOther, storageDPDirPath, partitionIdOther, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, readBy);						
					
					dpSourceOther.shuffleByValues(dmshuffle);
  					// add filtered entries from dpSourceOther to dp2
					dpSourceOther.begin();
  		  			while (dpSourceOther.hasNext()) {
  		  				DataElement e = dpSourceOther.next();
  		  				if (dpSourceOther.isCurrentElementBelongsToSU(tu, cu)) {
  		  					dp2.append(e);
  		  				}
  		  			}
  		  			dpSourceOther.close();
  		  			dpSourceOther = null;
  				}
  			}
  			
  		} catch(Throwable e) {
  			cr.detach();
  			ct.detach();
  			pool = null;
  			result = DMJson.error("CSE.EXE.DEFO3.1", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
  			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
  		}
  		// Release CtClass from ClassPool
  		cr.detach();
  		ct.detach();
  		pool = null;
	 		
  		log.debug("dataExecuteForShuffle_1_2() shuffle done");
  		
  		return DMJson.ok();	
	}
	
	public static String dataExecuteForShuffle_2_1(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId, String storageDPDirPath, String localDPDirPathForThisSplitRetryNum, IStorageHandler storageHandler) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException, DScabiException {
		DMJson djsonBy = new DMJson();
		djsonBy.add("AppId", dctx1.getAppId());
		djsonBy.add("SplitUnit", "" + dctx1.getDU());
		djsonBy.add("RetryNumber", "" + dctx1.getRetryNumber());
		djsonBy.add("ParallelNumber", "" + dctx1.getParallelNumber());
		String readBy = djsonBy.toString();
		
  		log.debug("dataExecuteForShuffle_2_1() TotalComputeUnit : {}", dj.getTU());
  		log.debug("dataExecuteForShuffle_2_1() SplitComputeUnit : {}", dj.getCU());
  		log.debug("dataExecuteForShuffle_2_1() JsonInput : {}", djson.getString("JsonInput"));
		
		long tu = dj.getTU();
		long cu = dj.getCU();
		
		String result = null;

		String appId = dj.getString("AppId");
		log.debug("dataExecuteForShuffle_2_1() AppId : {}", appId);
		String dataId1 = djson.getString("SourceDataId");
		log.debug("dataExecuteForShuffle_2_1() DataId1 : {}", dataId1);
		
  		try {
  			String jsonStrFieldNamesToGroup = djson.getString("JsonStrFieldNamesToGroup");
  			log.debug("dataExecuteForShuffle_2_1() jsonStrFieldNamesToGroup : {}", jsonStrFieldNamesToGroup);
  			DMJson djsonFieldNamesToGroup = new DMJson(jsonStrFieldNamesToGroup);
  			// TODO set a read only attribute to dp1.setReadOnly()
  			// create DataPartition object for other source dp with partition id dataid1_i_appid of source dataset dataId1
  			for (int i = 1; i <= tu; i++) {
  				
  				if (i == cu) {
  					// use dp1
  		  			dp1.shuffleByFieldNames(djsonFieldNamesToGroup);
  					// add filtered entries from dp1 to dp2
  		  			dp1.begin();
  		  			while (dp1.hasNext()) {
  		  				DataElement e = dp1.next();
  		  				if (dp1.isCurrentElementBelongsToSU(tu, cu)) {
  		  					dp2.append(e);
  		  				}
  		  			}
  		  			dp1.clearShuffle();
  				} else {
  					// check if other source dp exists and create DataPartition object if other source dp exists
  					String partitionIdOther = dataId1 + "_" + i + "_" + appId.replace("_", "");
  					log.debug("dataExecuteForShuffle_2_1() PartitionIdOther : {}", partitionIdOther);
  					DataPartition dpSourceOther = null;
					dpSourceOther = DataPartition.readDataPartition(dctx1, dataId1, partitionIdOther, storageDPDirPath, partitionIdOther, 64 * 1024 * 1024, localDPDirPathForThisSplitRetryNum, storageHandler, readBy);						
					
					dpSourceOther.shuffleByFieldNames(djsonFieldNamesToGroup);
  					// add filtered entries from dpSourceOther to dp2
					dpSourceOther.begin();
  		  			while (dpSourceOther.hasNext()) {
  		  				DataElement e = dpSourceOther.next();
  		  				if (dpSourceOther.isCurrentElementBelongsToSU(tu, cu)) {
  		  					dp2.append(e);
  		  				}
  		  			}
  		  			dpSourceOther.close();
  		  			dpSourceOther = null;
  				}
  			}
  			
  		} catch(Throwable e) {
  			result = DMJson.error("CSE.EXE.DEFS2.1", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
  			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
  		}
	 		
  		log.debug("dataExecuteForShuffle_2_1() shuffle done");
  		
  		return DMJson.ok();	
		
	}

	public static String dataExecuteForComparator_1_1(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException {

		return DMJson.ok();
	}
	
	public static String dataExecuteForComparator_1_2(DMClassLoader dcl, DMJson dj, DMJson djson, String className, byte[] b2, DataContext dctx1, DataPartition dp1, DataPartition dp2, String taskId) throws CannotCompileException, InstantiationException, IllegalAccessException, IOException, RuntimeException, NoSuchMethodException, NotFoundException {
		
		return DMJson.ok();
	}
	
	public static String computeExecuteCode(String request) {
		gc();
	    String s = null;
	    DMClassLoader dcl = null;
	    String result = null;
	    DMJson djson = null;
	    String taskId = null;
	    
	    log.debug("executeCode() request : {}", request);
	       try {
	    	   djson = new DMJson(request);
	    	   taskId = djson.getString("TaskId");
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }	    	   
	    	   
	    	   dcl = new DMClassLoader();
	    	   if(false == djson.contains("AddJars")) {
	    		   log.debug("executeCode() Information only : No additional jars to load. AddJars are not provided.");
	    	   } else {
	    		   log.debug("executeCode() AddJars are provided");
	    		   loadAddJars(dcl, djson);
	  				
	    		   //=====Debugging purpose only==========
	    		   //log.debug("executeCode() Going to load class TestNew");
	    		   //Class<?> testNew = dcl.loadClass("test.TestNew");
	    		   //log.debug("executeCode() Loaded class TestNew with name : {}", testNew.getName());
	    		   //=====================================
	    	   }
	    	   
	    	   log.debug("executeCode() TotalComputeUnit : {}", djson.getTU());
	    	   log.debug("executeCode() SplitComputeUnit : {}", djson.getCU());
	    	   log.debug("executeCode() JsonInput : {}", djson.getString("JsonInput"));
		  	   
	    	   /* Previous works
	    	   Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
	    	   Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	    	   Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
	           */
	    	   
	    	   DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
	    	   DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	    	   dson2.add("JobId", djson.getString("JobId"));
	    	   dson2.add("ConfigId", djson.getString("ConfigId"));
	    	   dson2.add("TaskId", djson.getString("TaskId"));
	    	   DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
	    	   
	    	   // Previous works DaoHelper ddaohelp = new DaoHelper("localhost", "27017", "MetaDB");
	    	   
	           Interpreter i = new Interpreter();
	           
	           // Not working i.setClassLoader(Thread.currentThread().getContextClassLoader());
	           // Not working i.setClassLoader(ClassLoader.getSystemClassLoader());
	           i.setClassLoader(dcl);

	           // Previous works i.set("jsonInput", dson3);
	           i.set("context", dson3);
	           i.eval("import com.mongodb.*");
	           i.eval("import com.dilmus.dilshad.scabi.common.*");
	           i.eval("import com.dilmus.dilshad.scabi.core.*");
	           
	           String code = null;
	           code = djson.getString("BshSource");
	           log.debug("executeCode() code : {}", code);
	           String pcode = DMUtil.preprocess(code);
	           log.debug("executeCode() pcode  : {}", pcode);
	           s = (String) i.eval(pcode);

	       }
	       catch ( TargetError e ) {
	    	   //s = e.toString();
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch ( ParseException e ) {
	    	   //s = e.toString();
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch ( EvalError e ) {
	    	   //s = e.toString();
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch (Error | RuntimeException e) {
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch (Exception e) {
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch (Throwable e) {
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }	    	   
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       }
	       result = DMJson.result(s);
    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }	    	   
    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
    	   return result;		
	}
	
	public static String computeExecuteClass(String request) {
		gc();
		DMClassLoader dcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		ClassPool pool = null;
		String result = null;   
		DMJson djson = null;
		String taskId = null;
		
		//log.debug("computeExecuteClass() request : {}", request);
			try {
				djson = new DMJson(request);
				taskId = djson.getString("TaskId");
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }			
				
			   String hexStr = djson.getString("ClassBytes");
			   //log.debug("computeExecuteClass() Hex string is : {}", hexStr);
			   String className = djson.getString("ClassName");
			   log.debug("computeExecuteClass() className is : {}", className);
			  	
			   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);

			   /* Previously used working code
			   dcl = new DClassLoader();
			   Class<?> df = dcl.findClass(className, b2);
			   ComputeUnit cuu = (ComputeUnit) df.newInstance();

			   log.debug("computeExecuteClass() TotalComputeUnit : {}", djson.getTU());
			   log.debug("computeExecuteClass() SplitComputeUnit : {}", djson.getSU());
			   log.debug("computeExecuteClass() JsonInput : {}", djson.getString("JsonInput"));
			   
			   Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
			   Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
			   Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			   
			   String result = cuu.compute(dson3);
			   dcl = null;
			   return DJson.result(result);
			   // return "" + result;
			   // return null; // returns HTTP/1.1 204 No Content
			   */
			   
			   	// Not used ClassLoader cl = ClassLoader.getSystemClassLoader();
		  		boolean proceed = false;
		  		DComputeUnit cuu = null;
		  		try {
		  			dcl = new DMClassLoader();
		  			Class<?> df = dcl.findClass(className, b2);
		  			
		  			if(false == djson.contains("AddJars")) {
		  				log.debug("computeExecuteClass() Information only : No additional jars to load. AddJars are not provided.");
		  				Thread.currentThread().setContextClassLoader(dcl);
		  			} else {
		  				log.debug("computeExecuteClass() AddJars are provided");
		  				loadAddJars(dcl, djson);
		  				
		  				//=====Debugging purpose only==========
		  				//log.debug("computeExecuteClass() Going to load class TestNew");
		  				//Class<?> testNew = dcl.loadClass("test.TestNew");
		  				//log.debug("computeExecuteClass() Loaded class TestNew with name : {}", testNew.getName());
		  				//=====================================
		  				
		  				Thread.currentThread().setContextClassLoader(dcl);

		  			}
		  			
			  		cuu = (DComputeUnit) df.newInstance();
			  		proceed = true;
		  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
		  			//e.printStackTrace();
		  			proceed = false;
		  		} catch (ClassCastException e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dcl = null;
		  			// Not needed System.gc();
		  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		  			return result;
		  		}
		  		
		  		log.debug("computeExecuteClass() TotalComputeUnit : {}", djson.getTU());
		  		log.debug("computeExecuteClass() SplitComputeUnit : {}", djson.getCU());
		  		log.debug("computeExecuteClass() JsonInput : {}", djson.getString("JsonInput"));
				
		  		/* Previous works
		  		Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		  		Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
				*/
		  		
		  		DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		    	dson2.add("JobId", djson.getString("JobId"));
		    	dson2.add("ConfigId", djson.getString("ConfigId"));
		    	dson2.add("TaskId", djson.getString("TaskId"));
		  		DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
		  		
		  		if (proceed) {
		  			log.debug("computeExecuteClass() ComputeUnit cast is working ok for this object");
			  		String res = null;
			  		try {
			  			res = cuu.compute(dson3);
			  		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			  		log.debug("computeExecuteClass() res : {}", res);
			  		Thread.currentThread().setContextClassLoader(originalLoader);
			  		dcl = null;
			  		// Not needed System.gc();
			  		result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
			  		
		  		} else {
		  			log.debug("computeExecuteClass() ComputeUnit cast is not working for this object. So proceeding with Class copy.");
		  			InputStream fis = new ByteArrayInputStream(b2);
			
					//works ClassPool pool = ClassPool.getDefault();
					pool = new ClassPool(true);
					pool.appendSystemPath();
					// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
					pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
					// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
					CtClass cr = pool.makeClass(fis);
			  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
			  		log.debug("computeExecuteClass() modifiers : {}", cr.getModifiers());
			  		
			  		// Reference CtClass ct = pool.getAndRename("com.dilmus.test.ComputeTemplate", "CT" + System.nanoTime());
			  		log.debug("computeExecuteClass() DMComputeTemplate.class.getCanonicalName() : {}", DMComputeTemplate.class.getCanonicalName());
			  		CtClass ct = pool.getAndRename(DMComputeTemplate.class.getCanonicalName(), "CT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
			  		   
				    CtMethod amethods[] = cr.getDeclaredMethods();
				    for (CtMethod amethod : amethods) {
				    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
				    	
				    	// OR
				    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
				    	//bmethod.setBody(amethod, null);
				    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
				    	// OR
				    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
				    	
				    	ct.addMethod(bmethod);
				    }
				    
				    CtField afields[] = cr.getDeclaredFields();
				    for (CtField afield : afields) {
				    	CtField bfield = new CtField(afield, ct);
					    ct.addField(bfield);
				    }
				    
				    // Notes : Anonymous class can not define constructor. So no need to copy constructor
				    
			  		Class<?> df2 = ct.toClass();
			  		Object ob = df2.newInstance();
			  		Method m = df2.getMethod("compute", DComputeContext.class);
			
			 		String res = null;
			 		try {
			 			res = (String) m.invoke(ob, dson3);
			 		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				  		cr.detach();
				 		ct.detach();
			  			pool = null;
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			 		// Release CtClass from ClassPool
			 		cr.detach();
			 		ct.detach();
			 		pool = null;
			 		dcl = null;
			 		
			  		log.debug("computeExecuteClass() res : {}", res);
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			// Not needed System.gc();
		  			result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
		  		}
		   
		   	} catch (Error | RuntimeException e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	  			pool = null;
	  			dcl = null;
	  			// Not needed System.gc();
	  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	  			return result;
		   	} catch(Exception e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
	  			dcl = null;
	  			// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	} catch(Throwable e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
	  			dcl = null;
	  			// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	}		
	}
	
	public static String computeExecuteFromObject(String request) {
		gc();
		DMClassLoader dcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		ClassPool pool = null;
		String result = null;
		DMJson djson = null;
		String taskId = null;
		
		//log.debug("computeExecuteFromObject() request : {}", request);
		   try {
			   djson = new DMJson(request);
			   taskId = djson.getString("TaskId");
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }
			   
			   String hexStr = djson.getString("ClassBytes");
			   //log.debug("computeExecuteFromObject() Hex string is : {}", hexStr);
			   String className = djson.getString("ClassName");
			   log.debug("computeExecuteFromObject() className is : {}", className);
			  	
			   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
			   log.debug("computeExecuteFromObject() to bytes  : {}", b2.toString());
			   /* for debugging
		  	   for (int i = 0; i < b.length; i++) {
		  			//log.debug("computeExecuteFromObject() b : {}, b2 : {}", b[i], b2[i]);
		  			if (b[i] != b2[i]) {
		  				log.debug("computeExecuteFromObject() b and b2 are not same");
		  				break;
		  			}
		  		}
		  		log.debug("computeExecuteFromObject() b and b2 are same");
			    */
		  		// Not used ClassLoader cl = ClassLoader.getSystemClassLoader() ;
		  		boolean proceed = false;
		  		DComputeUnit cuu = null;
		  		try {
		  			dcl = new DMClassLoader();
		  			Class<?> df = dcl.findClass(className, b2);
		  			
		  			if(false == djson.contains("AddJars")) {
		  				log.debug("computeExecuteFromObject() Information only : No additional jars to load. AddJars are not provided.");
		  				Thread.currentThread().setContextClassLoader(dcl);
		  			} else {
		  				log.debug("computeExecuteFromObject() AddJars are provided");
		  				loadAddJars(dcl, djson);
		  				
		  				//=====Debugging purpose only==========
		  				//log.debug("computeExecuteFromObject() Going to load class TestNew");
		  				//Class<?> testNew = dcl.loadClass("test.TestNew");
		  				//log.debug("computeExecuteFromObject() Loaded class TestNew with name : {}", testNew.getName());
		  				//Class<?> mpcu = dcl.loadClass("MyPrimeCheckUnit");
		  				//log.debug("computeExecuteFromObject() Loaded class with name : {}", mpcu.getName());
		  				//=====================================
		  				
		  				Thread.currentThread().setContextClassLoader(dcl);
		  			}
		  			
			  		cuu = (DComputeUnit) df.newInstance();
			  		proceed = true;
		  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
		  			// e.printStackTrace();
		  			proceed = false;
		  		} catch (ClassCastException e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dcl = null;
		  			// Not needed System.gc();
		  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		  			return result;
		  		}
		  		
		  		log.debug("computeExecuteFromObject() TotalComputeUnit : {}", djson.getTU());
		  		log.debug("computeExecuteFromObject() SplitComputeUnit : {}", djson.getCU());
		  		log.debug("computeExecuteFromObject() JsonInput : {}", djson.getString("JsonInput"));
				   
		  		/* Previous works
		  		Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		  		Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
		  		*/
		  		
		  		DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		    	dson2.add("JobId", djson.getString("JobId"));
		    	dson2.add("ConfigId", djson.getString("ConfigId"));
		    	dson2.add("TaskId", djson.getString("TaskId"));
		  		DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
		  		
		  		if (proceed) {
		  			log.debug("computeExecuteFromObject() ComputeUnit cast is working ok for this object");
			  		String res = null; 
			  		try {
			  			res = cuu.compute(dson3);
			  		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			  		log.debug("computeExecuteFromObject() res : {}", res);
			  		Thread.currentThread().setContextClassLoader(originalLoader);
			  		dcl = null;
			  		// Not needed System.gc();
			  		result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
			  		
		  		} else {
		  			log.debug("computeExecuteFromObject() ComputeUnit cast is not working for this object. So proceeding with Class copy.");
		  			InputStream fis = new ByteArrayInputStream(b2);
			
					//works ClassPool pool = ClassPool.getDefault();
					pool = new ClassPool(true);
					pool.appendSystemPath();
					// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
					pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
					// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
					CtClass cr = pool.makeClass(fis);
			  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
			  		log.debug("computeExecuteFromObject() modifiers : {}", cr.getModifiers());
			  		
			  		// Reference CtClass ct = pool.getAndRename("com.dilmus.test.ComputeTemplate", "CT" + System.nanoTime());
			  		log.debug("computeExecuteFromObject() DMComputeTemplate.class.getCanonicalName() : {}", DMComputeTemplate.class.getCanonicalName());
			  		CtClass ct = pool.getAndRename(DMComputeTemplate.class.getCanonicalName(), "CT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
			  		
			  		// Reference
			  		// CtMethod amethod = cr.getDeclaredMethod("compute");
				    // CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
				    // log.debug("computeExecuteFromObject() bmethod.getDeclaringClass() : {}", bmethod.getDeclaringClass()); 
				    // ct.addMethod(bmethod);
				    
				    CtMethod amethods[] = cr.getDeclaredMethods();
				    for (CtMethod amethod : amethods) {
				    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
				    	
				    	// OR
				    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
				    	//bmethod.setBody(amethod, null);
				    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
				    	// OR
				    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
				    	
				    	ct.addMethod(bmethod);
				    }
				    
				    CtField afields[] = cr.getDeclaredFields();
				    for (CtField afield : afields) {
				    	CtField bfield = new CtField(afield, ct);
					    ct.addField(bfield);
				    }
				    
				    // Notes : Anonymous class can not define constructor. So no need to copy constructor
				    
			  		Class<?> df2 = ct.toClass();
			  		if (null == df2)
			  			System.out.println("null == df2");

			  		Object ob = df2.newInstance();
			  		if (null == ob)
			  			System.out.println("null == ob");

			  		Method m = df2.getMethod("compute", DComputeContext.class);
			  		if (null == m)
			  			System.out.println("null == m");

			 		String res = null;
			 		try {
			 			res = (String) m.invoke(ob, dson3);
			 		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				  		//cr.detach();
				 		//ct.detach();
			  			pool = null;
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			 		// Release CtClass from ClassPool
			 		/*
			 		java.lang.NullPointerException
			 		at java.util.Hashtable.put(Hashtable.java:459)
			 		at javassist.ClassPool.cacheCtClass(ClassPool.java:258)
			 		at javassist.CtClass.detach(CtClass.java:1350)
			 		at com.dilmus.dilshad.scabi.cns.ComputeServer.computeExecuteFromObject(ComputeServer.java:411)
			 		*/
			 		//cr.detach();
			 		//ct.detach();
			 		pool = null;
			 		dcl = null;
			 		
			  		log.debug("computeExecuteFromObject() res : {}", res);
			  		Thread.currentThread().setContextClassLoader(originalLoader);
			  		// Not needed System.gc();
			  		result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
			  		// // // return "" + result;
		  		}
		  		// // // return null; // returns HTTP/1.1 204 No Content
			   
		   } catch (Error | RuntimeException e) {
			   //e.printStackTrace();
			   Thread.currentThread().setContextClassLoader(originalLoader);
			   pool = null;
			   dcl = null;
			   // Not needed System.gc();
			   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
			   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   return result;
		   	} catch(Exception e) {
		   		//e.printStackTrace();
		   		Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
		   		dcl = null;
		   		// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	} catch(Throwable e) {
		   		//e.printStackTrace();
		   		Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
		   		dcl = null;
		   		// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return DMJson.error(DMUtil.serverErrMsg(e));
		   	}		
	}
	
	public static String computeExecuteClassNameInJar(String request) {
		gc();
		DMClassLoader dmcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		String result = null;
		DMJson djson = null;
		String taskId = null;
		
		//log.debug("computeExecuteClassNameInJar() request : {}", request);
		   try {
			   djson = new DMJson(request);
			   taskId = djson.getString("TaskId");
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }
			   
			   String hexStr = djson.getString("JarBytes");
			   //log.debug("computeExecuteClass() Hex string is : {}", hexStr);
			   String classNameInJar = djson.getString("ClassNameInJar");
			   log.debug("computeExecuteClassNameInJar() classNameInJar is : {}", classNameInJar);
			   String jarFilePath = djson.getString("JarFilePath");
			   log.debug("computeExecuteClassNameInJar() JarFilePath is : {}", jarFilePath);
			  	
			   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
	  		
			   log.debug("computeExecuteClassNameInJar() TotalComputeUnit : {}", djson.getTU());
			   log.debug("computeExecuteClassNameInJar() SplitComputeUnit : {}", djson.getCU());
			   log.debug("computeExecuteClassNameInJar() JsonInput : {}", djson.getString("JsonInput"));

			   /* Previous works
			   Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
			   Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
			   Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			   */
			   
			   DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
			   DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	    	   dson2.add("JobId", djson.getString("JobId"));
	    	   dson2.add("ConfigId", djson.getString("ConfigId"));
	    	   dson2.add("TaskId", djson.getString("TaskId"));
			   DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			   
			   DComputeUnit cuu = null;
					
			   dmcl = new DMClassLoader();
			   String s = dmcl.loadJarAndSearchClass(jarFilePath, b2, classNameInJar);
			   if (null == s) {
				   log.debug("computeExecuteClassNameInJar() No match found for given class name : {}", classNameInJar);
				   result = DMJson.error("CSR.SYM.EXE", "No match found for given class name : " + classNameInJar);
				   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   return result;
			   } else {
				   log.debug("computeExecuteClassNameInJar() found first matching name for given class name : {}, matching class name : {}", classNameInJar, s);
			   }
			   Class<?> df = dmcl.loadClass(s);
	 			
			   if(false == djson.contains("AddJars")) {
				   log.debug("computeExecuteClassNameInJar() Information only : No additional jars to load. AddJars are not provided.");
				   Thread.currentThread().setContextClassLoader(dmcl);
			   } else {
				   log.debug("computeExecuteClassNameInJar() AddJars are provided");
				   DMExecute.loadAddJars(dmcl, djson);
	  				
				   //=====Debugging purpose only==========
				   //log.debug("Going to load class TestNew");
				   //Class<?> testNew = dcl.loadClass("test.TestNew");
				   //log.debug("Loaded class TestNew with name : {}", testNew.getName());
				   //=====================================
				   
	 				Thread.currentThread().setContextClassLoader(dmcl);

			   }
			   
			   cuu = (DComputeUnit) df.newInstance();
			   log.debug("computeExecuteClassNameInJar() Going to invoke method");
			   // Not used Dson dson = new Dson("input", "1");
			   String res = null;
			   try {
				   res = cuu.compute(dson3);
			   } catch(Throwable e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dmcl = null;
		  			// Not needed System.gc();
			   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
			   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   		return result;
			   }
			   log.debug("computeExecuteClassNameInJar res : {}", res);
			   
			   Thread.currentThread().setContextClassLoader(originalLoader);
			   dmcl = null;
			   // Not needed System.gc();
			   result = DMJson.result(res);
			   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
			   return result;
	 		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
	 			e.printStackTrace();
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
	  		} catch (ClassCastException e) {
	 			e.printStackTrace();
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
	 		} catch (Error | RuntimeException e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
		   	} catch(Exception e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
		   	} catch(Throwable e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
		   	}		
	}
	
   public static int loadAddJars(DMClassLoader dmcl, DMJson djson) throws IOException, DScabiException {
	   if (false == djson.contains("AddJars"))
		   throw new DScabiException("AddJars key not found in input json", "CSR.LAJ.1");

	   String jsonStrAddJars = djson.getString("AddJars");
	   DMJson djsonAddJars = new DMJson(jsonStrAddJars);
	   Set<String> st = djsonAddJars.keySet();
	   
	   for (String s : st) {
		   log.debug("loadAddJars() loading jar no.{}", s); 
		   String hexStr = djsonAddJars.getString(s);
		   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
		   dmcl.loadJar(s, b2);
	   }
	   return 0;
	   
   }	
	
	
	
	
	
	
}
