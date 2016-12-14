/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 18-Jul-2016
 * File Name : Data.java
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

package com.dilmus.dilshad.scabi.core.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
//import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMLambdaUtil;
import com.dilmus.dilshad.scabi.common.DMSeaweedStorageHandler;
import com.dilmus.dilshad.scabi.common.DMStdStorageHandler;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataPartition;
import com.dilmus.dilshad.scabi.core.DataUnit;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.IComparator;
import com.dilmus.dilshad.scabi.core.IForEachDataElement;
import com.dilmus.dilshad.scabi.core.IForEachDataPartition;
import com.dilmus.dilshad.scabi.core.IOperator;
import com.dilmus.dilshad.scabi.core.IOperator2;
import com.dilmus.dilshad.scabi.core.IShuffle;
import com.dilmus.dilshad.scabi.core.IShuffle2;
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;
import com.dilmus.dilshad.scabi.deprecated.DOperatorConfig_2_1;
import com.dilmus.dilshad.scabi.deprecated.DOperatorConfig_2_2;
import com.dilmus.dilshad.storage.IStorageHandler;

/**
 * @author Dilshad Mustafa
 *
 */
public class Data implements Runnable {

	private final Logger log = LoggerFactory.getLogger(Data.class);
	private ExecutorService m_threadPool = null;
	private DMeta m_meta = null;
	private long m_commandID = 2;
	private HashMap<String, DataAsyncConfigNode> m_commandMap = null;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private long m_maxSplit = 1;
	private int m_maxRetry = 3; //0;
	private long m_maxThreads = 0; //1;
	private long m_splitTotal = 0;
	private DataAsyncConfigNode m_configNode = null;
	private boolean m_isPerformInProgress = false;
	private boolean m_crunListReady = false;
	private boolean m_futureListReady = false;

	private LinkedList<DataAsyncConfigNode> m_configNodeList = null;
	private LinkedList<DataAsyncRun_D2> m_crunList = null;
	private LinkedList<DataNoBlock> m_cnbList = null;
	
	private LinkedList<Future<?>> m_futureList = null;
	private HashMap<Future<?>, DataRangeRunner_D2> m_futureRRunMap = null;
	
	private Future<?> m_futureCompute = null;
	private Future<?> m_futureRetry = null;
	
	private long m_noOfRangeRunners = 0;
	
	private DataRetryAsyncMonitor_D2 m_retryAsyncMonitor = null;
	
	private boolean m_isJarFilePathListSet = false;
	private LinkedList<String> m_jarFilePathList = null;
	
	private String m_emptyJsonStr = Dson.empty();
	
	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	private long m_crunListSize = 0;
	private long m_cnbListSize = 0;
	private long m_futureListSize = 0;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	private final static int DIT_DATAUNIT_CLASS = 1;
	private final static int DIT_DATAUNIT_CLASS_OF_INTERFACE = 2;
	private final static int DIT_DATAUNIT_OBJECT = 3;
	private final static int DIT_DATAUNIT_OBJECT_OF_INTERFACE = 4;
	// For future features, repartition from another Data cluster
	private final static int DIT_PARTITIONER_CLASS = 5;
	private final static int DIT_PARTITIONER_CLASS_OF_INTERFACE = 6;
	private final static int DIT_PARTITIONER_OBJECT = 7;
	private final static int DIT_PARTITIONER_OBJECT_OF_INTERFACE = 8;
	
	private int m_dataInitiatorType = 0; // one of the DIT_ types above
	private int m_configNodeType = 0; // DataAsyncConfigNode type

	private String m_dataId = null;
	private Class<? extends DataUnit> m_dataUnitClass = null;
	private DataUnit m_dataUnitObject = null;
	// For future features, repartition from another Data cluster
	private Class<? extends DPartitioner> m_partitionerClass = null;
	private DPartitioner m_partitionerObject = null;
	
	private long m_startCommandId = 1;
	private long m_endCommandId = 1;
	
	private LinkedList<DataAsyncRun_D2> m_sourceCRunList = null;
	private long m_sourceCRunListSize = 0;
	private boolean m_isSourceCRunListSet = false;
	private String m_sourceCRunListJsonStr = null;

	private String m_dataId1 = null;
	private String m_dataId2 = null;
	
	private String m_appName = "My App";
	private String m_appId = null;
	
	private String m_lambdaMethodName = null;
	
	private String m_localDirPath = null;
	private String m_storageProvider = null;
	private String m_mountDirPath = null;
	private String m_storageConfig = null;
	private IStorageHandler m_storageHandler = null;
	
	private boolean m_isSpecificInput = false;
	private String m_specificJsonStrInput = null;
	
	public static int deleteData(String appId, String dataId, IStorageHandler storageHandler) throws Exception {
		// TODO yet to be unit tested
		// TODO read noOfSplits from text file <appId>.txt
		// TODO this <appId>.txt should have been created earlier in Storage System in either perform() or finish() method
		storageHandler.copyIfExistsToLocal(appId + File.separator + appId + ".txt", appId + ".txt");
		FileInputStream fis = new FileInputStream(appId + ".txt");
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
		String s = br.readLine();
		
		br.close();
		isr.close();
		fis.close();
		
		long noOfSplits = Long.parseLong(s);
		
		deleteData(appId, dataId, noOfSplits, storageHandler);
		
		Path path = Paths.get(appId + ".txt");
		Files.deleteIfExists(path);
		
		return 0;
	}
	
	public static int deleteData(String appId, String dataId, long noOfsplits, IStorageHandler storageHandler) throws DScabiException, IOException {
		// TODO yet to be unit tested
		String storageProvider = null;
		String mountDirPath = null;
		String storageConfig = null;
		String storageDPDirPath = null;
 		
		Logger log = LoggerFactory.getLogger(Data.class);
		
 		storageProvider = System.getProperty("scabi.storage.provider");
 		if (storageProvider != null && storageProvider.length() > 0) {
	 		log.debug("deleteData(...) Property scabi.storage.provider, storageProvider : {}", storageProvider);
 			if (storageProvider.equalsIgnoreCase("dfs")) {
 				mountDirPath = System.getProperty("scabi.dfs.mount.dir");
 		 		if (null == mountDirPath || mountDirPath.length() == 0) {
 		 			log.error("deleteData(...) Mount or storage directory is not specified in property scabi.dfs.mount.dir");
 		 			throw new DScabiException("Mount or storage directory is not specified in property scabi.dfs.mount.dir", "DAA.DDD.1");
 		 		}
 		 		log.debug("deleteData(...) Property scabi.dfs.mount.dir, mountDirPath : {}", mountDirPath);

 			} else if (storageProvider.equalsIgnoreCase("seaweedfs")) {
				storageConfig = System.getProperty("scabi.seaweedfs.config");
		 		if (null == storageConfig || storageConfig.length() == 0) {
		 			log.error("deleteData(...) Config is not specified in property scabi.seaweedfs.config");
 		 			throw new DScabiException("Config is not specified in property scabi.seaweedfs.config", "DAA.DDD.1");		 			
		 		}
		 		log.debug("deleteData(...) Property scabi.seaweedfs.config, storageConfig : {}", storageConfig);

 			} else {
 	 			log.error("deleteData(...) Invalid Storage Provider is specified in property scabi.storage.provider : {}", storageProvider);
 	 			log.error("deleteData(...) Valid Storage Provider values : dfs, fuse, nfs, seaweedfs");
		 		throw new DScabiException("Invalid Storage Provider is specified in property scabi.storage.provider : " + storageProvider, "DAA.DDD.1");		 			
 			}
 		} else {
 			log.error("deleteData(...) Storage Provider is not specified in property scabi.storage.provider");
	 		throw new DScabiException("Storage Provider is not specified in property scabi.storage.provider", "DAA.DDD.1");		 			
 		}
		
		if (storageProvider.equalsIgnoreCase("dfs") 
			|| storageProvider.equalsIgnoreCase("fuse")
			|| storageProvider.equalsIgnoreCase("nfs")) 
		{
  			storageDPDirPath = mountDirPath;
		} else if (storageProvider.equalsIgnoreCase("seaweedfs")) {
			storageDPDirPath = appId; // For storage system that can not create directory, for example DMSeaweedStorageHandler
		} else {
			throw new DScabiException("Unknown StorageProvider : " + storageProvider, "DAA.GDP.1");
		}
			
    	long n = noOfsplits;
    	for (long splitUnit = 1; splitUnit <= n; splitUnit++) {
	    	DataPartition.deletePartition(appId, dataId, splitUnit, storageDPDirPath, storageHandler);
    	}
	    	
    	return 0;
	}
	
	public int deleteData(String dataId) throws DScabiException, IOException {
		
		String storageDPDirPath = null;
		
  		// For storage system that can create directory
		// Pass the storage directory path through m_mountDir from ComputeServer_D2.getStorageDir() in case of DMHdfsStorageHandler
		if (m_storageProvider.equalsIgnoreCase("dfs") 
			|| m_storageProvider.equalsIgnoreCase("fuse")
			|| m_storageProvider.equalsIgnoreCase("nfs")) 
		{
  			storageDPDirPath = m_mountDirPath;
		} else if (m_storageProvider.equalsIgnoreCase("seaweedfs")) {
			storageDPDirPath = m_appId; // For storage system that can not create directory, for example DMSeaweedStorageHandler
		} else {
			throw new DScabiException("Unknown StorageProvider : " + m_storageProvider, "DAA.GDP.1");
		}
		
    	long n = m_splitTotal;
    	for (long splitUnit = 1; splitUnit <= n; splitUnit++) {
	    	DataPartition.deletePartition(m_appId, dataId, splitUnit, storageDPDirPath, m_storageHandler);
    	}
    	
    	return 0;
	}
	
	public long getNoOfSplits() {
		return m_splitTotal;
	}
	
	public DataPartition getDataPartition(String dataId, long splitUnit) throws Exception {
		
  		String localDPDirPath = m_localDirPath;
  		
		String storageDPDirPath = null;
		IStorageHandler  storageHandler = null;
		
  		// For storage system that can create directory
		// Pass the storage directory path through m_mountDir from ComputeServer_D2.getStorageDir() in case of DMHdfsStorageHandler
		if (m_storageProvider.equalsIgnoreCase("dfs") 
			|| m_storageProvider.equalsIgnoreCase("fuse")
			|| m_storageProvider.equalsIgnoreCase("nfs")) 
		{
  			storageDPDirPath = m_mountDirPath;
		} else if (m_storageProvider.equalsIgnoreCase("seaweedfs")) {
			storageDPDirPath = m_appId; // For storage system that can not create directory, for example DMSeaweedStorageHandler
		} else {
			throw new DScabiException("Unknown StorageProvider : " + m_storageProvider, "DAA.GDP.1");
		}

		DataContext dctx = DataContext.dummy();
		String partitionId = dataId + "_" + splitUnit + "_" + m_appId.replace("_", "");
		DataPartition dp = new DataPartition(dctx, dataId, partitionId, storageDPDirPath, partitionId, 64 * 1024 * 1024, localDPDirPath, m_storageHandler);

		return dp;
	}
	
	private void init() throws IOException {
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
 	  			m_storageHandler = new DMStdStorageHandler();
 			} else if (m_storageProvider.equalsIgnoreCase("seaweedfs")) {
				m_storageConfig = System.getProperty("scabi.seaweedfs.config");
		 		if (null == m_storageConfig || m_storageConfig.length() == 0) {
		 			log.error("init() Config is not specified in property scabi.seaweedfs.config");
		 			System.exit(0);
		 		}
		 		log.debug("init() Property scabi.seaweedfs.config, m_storageConfig : {}", m_storageConfig);
				m_storageHandler = new DMSeaweedStorageHandler(m_storageConfig);
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
	
	
	public LinkedList<DataAsyncConfigNode> getConfigNodeList() {
		return m_configNodeList;
	}
	
	public LinkedList<DataAsyncRun_D2> getCRunList() {
		return m_crunList;
	}
	
	public LinkedList<DataNoBlock> getCNBList() {
		return m_cnbList;
	}
	
	public ExecutorService getExecutorService() {
		return m_threadPool;
	}
	
	public int putFutureRRunMap(Future<?> f, DataRangeRunner_D2 rrun) {
		// TODO sync on m_futureRRunMap should be sufficient
		synchronized (this) {
			m_futureRRunMap.put(f, rrun);
		}
		return 0;
	}
	
	public DataRangeRunner_D2 getFutureRRunMap(Future<?> f) {
		// TODO sync on m_futureRRunMap should be sufficient
		synchronized (this) {
			return m_futureRRunMap.get(f);
		}
	}

	public HashMap<Future<?>, DataRangeRunner_D2> getFutureRRunMap() {
			return m_futureRRunMap;
	}
	
	public int addJar(String jarFilePath) throws DScabiException {
		if (null == jarFilePath)
			throw new DScabiException("jarFilePath is null", "CAC.AJR.1");
		m_jarFilePathList.add(jarFilePath);
		m_isJarFilePathListSet = true;
		return 0;
		
	}
	
	public int addComputeUnitJars() throws DScabiException {
		
		if (false == Thread.currentThread().getContextClassLoader() instanceof DMClassLoader) {
			throw new DScabiException("This thread context class loader is not of DMClassLoader", "CAC.ACU.1");
		}
		m_isComputeUnitJarsSet = true;
		m_dcl = (DMClassLoader)Thread.currentThread().getContextClassLoader();
		return 0;
	}
	
	/* Reference : use below code as reference for executeClass() in DCompute class
	public Data(DMeta meta, String dataId, Class<?> cls) throws DScabiException {
		
		// Use below code as reference for executeClass() method in DCompute class
		// executeClass() { if class of DComputeUnit then call executeClassOfClass()
		// else if class of IComputeUnit then call executeClassOfInterface() }
		
		boolean isClassMatchFound = false;
		boolean isInterfaceMatchFound = false;
    	log.debug("Super class : {}", cls.getGenericSuperclass());
    	Type typeSuper = cls.getGenericSuperclass();
    	log.debug("Type name : {}", typeSuper.getTypeName());
    	log.debug("Data Unit class canonical name : {}", DataUnit.class.getCanonicalName());
    	// Don't use typeSuper.getClass().getCanonicalName().contains(DataUnit.class.getCanonicalName())
    	// as typeSuper.getClass() returns java.lang.Class
    	if (typeSuper.getTypeName().contains(DataUnit.class.getCanonicalName())) {
    		isClassMatchFound = true;
    		log.debug("isClassMatchFound : {}", isClassMatchFound);
    		m_configType = Data.CLASS;
    	} else {
        	// Don't use .getClass on Type object
    		// as it returns java.lang.Class
    		Type clsa[] = cls.getGenericInterfaces();
    		System.out.println("clsa length : " + clsa.length);
    		isInterfaceMatchFound = false;
    		for (Type clse : clsa) {
    			System.out.println(clse.getTypeName());
			    if (clse.getTypeName().contains("IDataUnit")) {
	    			// OR clse.getTypeName().contains(IDataUnit.class.getCanonicalName())
	    			// for example Serializable.class.getCanonicalName()
	    			isInterfaceMatchFound = true;
	        		log.debug("isInterfaceMatchFound : {}", isInterfaceMatchFound);
	        		m_configType = Data.CLASS_OF_INTERFACE;
	    			break;
			    }
    		}

    		// OR below code, works ok
	    	// Class<?> clsa[] = cls.getInterfaces();
	    	// System.out.println("clsa length : " + clsa.length);
	    	// isInterfaceMatchFound = false;
	    	// for (Class<?> clse : clsa) {
	    	// 	System.out.println(clse.getCanonicalName());
	    	// 	if (clse.getCanonicalName().contains("IDataUnit")) {
	    	//		// OR clse.getCanonicalName().contains(IDataUnit.class.getCanonicalName())
	    	//		// for example Serializable.class.getCanonicalName()
	    	//		isInterfaceMatchFound = true;
	        //		log.debug("isInterfaceMatchFound : {}", isInterfaceMatchFound);
	        //		m_configType = Data.CLASS_OF_INTERFACE;
	    	//		break;
	    	// 	}
	    	// }
	    	
    	}
    	
    	if (false == isClassMatchFound && false == isInterfaceMatchFound) {
    		log.error("Data() Class is not subclass of DataUnit or implements interface IDataUnit");
    		throw new DScabiException("Class is not subclass of DataUnit or implements interface IDataUnit", "Driver.DAT.DAT");
    	}
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DataAsyncConfig>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 0; //1;
		m_splitTotal = 0;
		
		m_cconfigList = new LinkedList<DataAsyncConfig>();
		m_crunList = new LinkedList<DataAsyncRun>();
		m_cnbList = new LinkedList<DataNoBlock>();
		
		m_futureList = new LinkedList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DataRangeRunner>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cnbListSize = 0;
		m_futureListSize = 0;
		
		m_dataId = dataId;
		m_dataClass = cls;
	
	}
	*/
	
	public Data(DMeta meta, String dataId, Class<?> cls) throws DScabiException, IOException {
		
		init();
		
		boolean isClassMatchFound = false;
    	log.debug("Data() Super class : {}", cls.getGenericSuperclass());
    	Type typeSuper = cls.getGenericSuperclass();
    	log.debug("Data() Type name : {}", typeSuper.getTypeName());

    	// Don't use typeSuper.getClass().getCanonicalName().contains(DataUnit.class.getCanonicalName())
    	// as typeSuper.getClass() returns java.lang.Class
    	if (typeSuper.getTypeName().contains(DataUnit.class.getCanonicalName())) {
    		isClassMatchFound = true;
    		log.debug("Data() isClassMatchFound : {}", isClassMatchFound);
  		
    		m_dataInitiatorType = DIT_DATAUNIT_CLASS;
    		m_dataId = dataId;
    		m_dataUnitClass = (Class<? extends DataUnit>)cls;
    		m_dataUnitObject = null;
    		m_partitionerClass = null;
    		m_partitionerObject = null;
    	} else if (typeSuper.getTypeName().contains(DPartitioner.class.getCanonicalName())) {
    		isClassMatchFound = true;
    		log.debug("isClassMatchFound : {}", isClassMatchFound);
  		
    		m_dataInitiatorType = DIT_PARTITIONER_CLASS;
    		m_dataId = dataId;
    		m_dataUnitClass = null;
    		m_dataUnitObject = null;
    		m_partitionerClass = (Class<? extends DPartitioner>)cls;
    		m_partitionerObject = null;
    	} else {
    		throw new DScabiException("Class is not DataUnit or DPartitioner type", "Driver.DAA.DAA.1");
    	}

		m_meta = meta;
		m_commandMap = new HashMap<String, DataAsyncConfigNode>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 0; //1;
		m_splitTotal = 0;
		
		m_configNodeList = new LinkedList<DataAsyncConfigNode>();
		m_crunList = new LinkedList<DataAsyncRun_D2>();
		m_cnbList = new LinkedList<DataNoBlock>();
		
		m_futureList = new LinkedList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DataRangeRunner_D2>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cnbListSize = 0;
		m_futureListSize = 0;
		
		m_configNodeType = 0;
		
		m_startCommandId = 1;
		m_endCommandId = 1;
		
		DataNoBlock.startHttpAsyncService();
		
		m_appId = UUID.randomUUID().toString();
		m_appId = m_appId.replace('-', '_');
	}	
	
	public Data(DMeta meta, String dataId, DataUnit dataUnitObj) throws DScabiException, IOException {

		init();
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DataAsyncConfigNode>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 0; //1;
		m_splitTotal = 0;
		
		m_configNodeList = new LinkedList<DataAsyncConfigNode>();
		m_crunList = new LinkedList<DataAsyncRun_D2>();
		m_cnbList = new LinkedList<DataNoBlock>();
		
		m_futureList = new LinkedList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DataRangeRunner_D2>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cnbListSize = 0;
		m_futureListSize = 0;
		
		m_dataInitiatorType = DIT_DATAUNIT_OBJECT;
		m_configNodeType = 0;
		m_dataId = dataId;
		m_dataUnitClass = null;
		m_dataUnitObject = dataUnitObj;
		m_partitionerClass = null;
		m_partitionerObject = null;
		
		m_startCommandId = 1;
		m_endCommandId = 1;
		
		DataNoBlock.startHttpAsyncService();
		
		m_appId = UUID.randomUUID().toString();
		m_appId = m_appId.replace('-', '_');
	}	
	
	public Data(DMeta meta, String dataId, DPartitioner partitionerObj) throws DScabiException, IOException {

		init();
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DataAsyncConfigNode>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 0; //1;
		m_splitTotal = 0;
		
		m_configNodeList = new LinkedList<DataAsyncConfigNode>();
		m_crunList = new LinkedList<DataAsyncRun_D2>();
		m_cnbList = new LinkedList<DataNoBlock>();
		
		m_futureList = new LinkedList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DataRangeRunner_D2>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cnbListSize = 0;
		m_futureListSize = 0;
		
		m_dataInitiatorType = DIT_PARTITIONER_OBJECT;
		m_configNodeType = 0;
		m_dataId = dataId;
		m_dataUnitClass = null;
		m_dataUnitObject = null;
		m_partitionerClass = null;
		m_partitionerObject = partitionerObj;
		
		m_startCommandId = 1;
		m_endCommandId = 1;
		
		DataNoBlock.startHttpAsyncService();
		
		m_appId = UUID.randomUUID().toString();
		m_appId = m_appId.replace('-', '_');
	}	
	
	public Data(DMeta meta, String dataId) throws IOException {

		init();
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DataAsyncConfigNode>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 0; //1;
		m_splitTotal = 0;
		
		m_configNodeList = new LinkedList<DataAsyncConfigNode>();
		m_crunList = new LinkedList<DataAsyncRun_D2>();
		m_cnbList = new LinkedList<DataNoBlock>();
		
		m_futureList = new LinkedList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DataRangeRunner_D2>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cnbListSize = 0;
		m_futureListSize = 0;
		
		m_dataInitiatorType = 0;
		m_configNodeType = 0;
		m_dataId = dataId;
		m_dataUnitClass = null;;
		m_dataUnitObject = null;
		m_partitionerClass = null;
		m_partitionerObject = null;

		m_startCommandId = 1;
		m_endCommandId = 1;
		
		DataNoBlock.startHttpAsyncService();
		
		m_appId = UUID.randomUUID().toString();
		m_appId = m_appId.replace('-', '_');
	}
	
	public int initialize() throws InterruptedException {
		
		m_configNode = null;
		
		m_isPerformInProgress = false;
		m_futureListReady = false;

		synchronized (this) {
			
			m_futureList.clear();
			m_futureRRunMap.clear();
			
			m_futureListSize = 0;
		}
		
		m_configNodeType = 0;
		
		m_startCommandId = m_commandID;
		m_endCommandId = m_commandID;
		
		return 0;
	}
	
	public int close() throws Exception {
		if (m_threadPool != null) {
			m_threadPool.shutdownNow();
			m_threadPool = null;			
		}
		closeCNBConnections();
		DataNoBlock.closeHttpAsyncService();
		
		m_storageHandler.close();
		
		return 0;
	}
	
	public long getCRunListSize() {
		return m_crunListSize;
	}
	
	public long getCNBListSize() {
		return m_cnbListSize;
	}

	private Data setDataUnitClass(Class<? extends DataUnit> cls) throws DScabiException, IOException {
		
		m_configNodeType = DataAsyncConfigNode.CNT_DATAUNIT_CONFIG;
		DataUnitConfig config = new DataUnitConfig(cls);
		m_configNode = new DataAsyncConfigNode(config);

		config.setDataId(m_dataId);
		config.setInput(m_jsonStrInput);
		config.setOutput(m_outputMap);
		config.setMaxSplit(m_maxSplit);
		config.setMaxRetry(m_maxRetry);
		
		m_commandMap.put("1", m_configNode);

		m_configNodeList.add(0, m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		return this;
	}
	
	private Data setDataUnitObject(DataUnit unit) throws DScabiException, IOException {
	
		m_configNodeType = DataAsyncConfigNode.CNT_DATAUNIT_CONFIG;
		DataUnitConfig config = new DataUnitConfig(unit);
		m_configNode = new DataAsyncConfigNode(config);
		
		config.setDataId(m_dataId);
		config.setInput(m_jsonStrInput);
		config.setOutput(m_outputMap);
		config.setMaxSplit(m_maxSplit);
		config.setMaxRetry(m_maxRetry);
		
		m_commandMap.put("1", m_configNode);
		
		m_configNodeList.add(0, m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		return this;
	}

	private Data setPartitionerClass(Class<? extends DPartitioner> cls) throws DScabiException, IOException {
		
		m_configNodeType = DataAsyncConfigNode.CNT_PARTITIONER_CONFIG;
		DMPartitionerConfig config = new DMPartitionerConfig(cls);
		m_configNode = new DataAsyncConfigNode(config);

		config.setDataId(m_dataId);
		config.setInput(m_jsonStrInput);
		config.setOutput(m_outputMap);
		config.setMaxSplit(m_maxSplit);
		config.setMaxRetry(m_maxRetry);
		
		m_commandMap.put("1", m_configNode);

		m_configNodeList.add(0, m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		return this;
	}
	
	private Data setPartitionerObject(DPartitioner unit) throws DScabiException, IOException {
		
		m_configNodeType = DataAsyncConfigNode.CNT_PARTITIONER_CONFIG;
		DMPartitionerConfig config = new DMPartitionerConfig(unit);
		m_configNode = new DataAsyncConfigNode(config);
		
		config.setDataId(m_dataId);
		config.setInput(m_jsonStrInput);
		config.setOutput(m_outputMap);
		config.setMaxSplit(m_maxSplit);
		config.setMaxRetry(m_maxRetry);
		
		m_commandMap.put("1", m_configNode);
		
		m_configNodeList.add(0, m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		return this;
	}
	
	private int setForOperatorConfig_1_1(DMOperatorConfig_1_1 config) {

		config.setSourceDataId(m_dataId1);
		config.setTargetDataId(m_dataId2);
		
		if (m_isSpecificInput) {
			config.setInput(m_specificJsonStrInput);
			m_isSpecificInput = false;
			m_specificJsonStrInput = null;
		}
		else
			config.setInput(m_jsonStrInput);
		
		config.setOutput(m_outputMap);
		config.setMaxSplit(m_maxSplit);
		config.setMaxRetry(m_maxRetry);

		return 0;
	}

	private int setForOperatorConfig_1_2(DMOperatorConfig_1_2 config) {

		config.setLambdaMethodName(m_lambdaMethodName);
		config.setSourceDataId(m_dataId1);
		config.setTargetDataId(m_dataId2);
		
		if (m_isSpecificInput) {
			config.setInput(m_specificJsonStrInput);
			m_isSpecificInput = false;
			m_specificJsonStrInput = null;	
		}
		else
			config.setInput(m_jsonStrInput);
		
		config.setOutput(m_outputMap);
		config.setMaxSplit(m_maxSplit);
		config.setMaxRetry(m_maxRetry);

		return 0;
	}

	public int setSourceCRunList(LinkedList<DataAsyncRun_D2> crunList, long crunListSize) {
		
		m_sourceCRunList = crunList;
		m_sourceCRunListSize = crunListSize;
		
		DMJson dj = new DMJson();
		long n = 1;
		DMJson djson = new DMJson();
		DataAsyncRun_D2 cr = crunList.getFirst();
		dj.add("TU", cr.getTU());
		dj.add("DRUNLISTSIZE", crunListSize);
		
		for (DataAsyncRun_D2 crun : crunList) {

			djson.clear();
			djson.add("DU", crun.getSU());
			djson.add("DNB", crun.getComputeNB().toString());
			
			dj.add("" + n, djson.toString());
			n++;
			
		}
		m_sourceCRunListJsonStr = dj.toString();
		log.debug("m_sourceCRunListJsonStr : {}", m_sourceCRunListJsonStr);
		m_isSourceCRunListSet = true;
		return 0;
	}

	public int createPartitions() {
		// This method is called from target Data object (the new Data object after repartitioning)
		// createPartitions is an action. Do it immediately and not through DRangeRunner
		// use all crun from crun list, get the cnb of all crun(s) and directly invoke
		// method on the cnb
		
		// Create DPartitionerConfig inside the target Data object (the new Data object after repartitioning)
		
		return 0;
	}
	
	public Data repartition(String newDataId, DPartitioner partitioner) throws DScabiException, IOException {
		// This method is called from source Data object
		// repartition is an action because all source Data Partitions in the source Data object
		// should be ready
		
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "DAA.OPE.1");
		}
		
		Data dataNew = new Data(m_meta, newDataId, partitioner);
		dataNew.setSourceCRunList(m_crunList, m_crunListSize);
		dataNew.createPartitions();
		
		// Do not create DPartitionerConfig inside the source Data object
		
		return dataNew;
	}
	
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	public Data operate(String dataId1, String dataId2, IOperator unit) throws Exception {
	
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "DAA.OPE.1");
		}

		if (m_configNode != null) {
			if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1 == m_configNode.getConfigNodeType()) {
				setForOperatorConfig_1_1(m_configNode.getOperatorConfig_1_1());
			} else if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2 == m_configNode.getConfigNodeType()) {
				setForOperatorConfig_1_2(m_configNode.getOperatorConfig_1_2());
			}
			
			m_commandMap.put("" + m_commandID, m_configNode);
			m_commandID++;
			
			m_configNodeList.add(m_configNode);
			
			m_configNode = null;
			m_configNodeType = 0;
			
			m_dataId1 = null;
			m_dataId2 = null;
			
		}
		
		if (unit.getClass().getName().contains("$$Lambda$")) {
			m_lambdaMethodName = DMLambdaUtil.getMethodName(unit);
			Class<?> cls = DMLambdaUtil.getImplClass(unit);

			m_configNodeType = DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2;
			DMOperatorConfig_1_2 config = new DMOperatorConfig_1_2(cls);
			m_configNode = new DataAsyncConfigNode(config);
			//System.exit(0);
		} else {
			m_configNodeType = DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1;
			DMOperatorConfig_1_1 config = new DMOperatorConfig_1_1(unit);
			m_configNode = new DataAsyncConfigNode(config);
		}
		m_dataId1 = dataId1;
		m_dataId2 = dataId2;
		
		return this;
	}
	
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	private Data lambda(String dataId1, String dataId2, IOperator2 unit) throws DScabiException, IOException {
		
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "DAA.OPE.1");
		}

		if (m_configNode != null) {
			if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1 == m_configNode.getConfigNodeType()) {
				setForOperatorConfig_1_1(m_configNode.getOperatorConfig_1_1());
			} else if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2 == m_configNode.getConfigNodeType()) {
				setForOperatorConfig_1_2(m_configNode.getOperatorConfig_1_2());
			}
			
			m_commandMap.put("" + m_commandID, m_configNode);
			m_commandID++;
			
			m_configNodeList.add(m_configNode);
			
			m_configNode = null;
			m_configNodeType = 0;
			
			m_dataId1 = null;
			m_dataId2 = null;
			
		}
		
		m_configNodeType = DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2;
		DMOperatorConfig_1_2 config = new DMOperatorConfig_1_2(unit);
		m_configNode = new DataAsyncConfigNode(config);
		m_dataId1 = dataId1;
		m_dataId2 = dataId2;
		
		return this;
	}	
	
	// TODO handy method at partition level
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	public Data foreachPartition(String dataId1, IForEachDataPartition fe) {
	
		return this;
	}
	
	// TODO handy method at DataElement level
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	public Data foreachElement(String dataId1, IForEachDataElement fe) {
		
		return this;
	}
	
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	public Data groupBy(String dataId1, String dataId2, IShuffle unit) throws DScabiException, IOException {
		shuffle(dataId1, dataId2, unit);
		return this;
	}
	
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	private Data shuffle(String dataId1, String dataId2, IShuffle unit) throws DScabiException, IOException {
		
		// this is to .perform() of previous operations
		m_endCommandId = m_commandID - 1;
		
		//.perform() of previous operations
		if (m_startCommandId <= m_endCommandId)
			perform();
		
		// Why shuffle is an action?
		// Because all data in the data partition of all crun(s) should be ready
		// before starting the shuffle

		// TODO shuffle is an action. Do it immediately and not through DRangeRunner
		// use all crun from crun list, get the cnb of all crun(s) and directly invoke
		// method on the cnb
		
		// Create shuffle config
		m_configNodeType = DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_1;
		DMShuffleConfig_1_1 config = new DMShuffleConfig_1_1(unit);
		m_configNode = new DataAsyncConfigNode(config);
		config.setSourceDataId(dataId1);
		config.setTargetDataId(dataId2);

		m_commandMap.put("" + m_commandID, m_configNode);
		m_commandID++;
		
		m_configNodeList.add(m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		m_dataId1 = null;
		m_dataId2 = null;
		
		m_startCommandId = m_commandID;
		m_endCommandId = m_commandID;
		
		return this;
	}
	
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	private Data shuffle(String dataId1, String dataId2, IShuffle2 unit) throws DScabiException, IOException {
		
		// this is to .perform() of previous operations
		m_endCommandId = m_commandID - 1;
		
		//.perform() of previous operations
		if (m_startCommandId <= m_endCommandId)
			perform();
		
		// Why shuffle is an action?
		// Because all data in the data partition of all crun(s) should be ready
		// before starting the shuffle

		// TODO shuffle is an action. Do it immediately and not through DRangeRunner
		// use all crun from crun list, get the cnb of all crun(s) and directly invoke
		// method on the cnb
		
		// Create shuffle config
		m_configNodeType = DataAsyncConfigNode.CNT_SHUFFLE_CONFIG_1_2;
		DMShuffleConfig_1_2 config = new DMShuffleConfig_1_2(unit);
		m_configNode = new DataAsyncConfigNode(config);
		config.setSourceDataId(dataId1);
		config.setTargetDataId(dataId2);

		m_commandMap.put("" + m_commandID, m_configNode);
		m_commandID++;
		
		m_configNodeList.add(m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		m_dataId1 = null;
		m_dataId2 = null;
		
		m_startCommandId = m_commandID;
		m_endCommandId = m_commandID;
		
		return this;
	}	
	
	// Note : Nested anonymous class and inner class defined inside anonymous class are not supported
	// Note : Nested lamda function-class and inner class defined inside lamda function-class are not supported
	private Data sort(String dataId1, String dataId2, IComparator<?> unit) throws DScabiException, IOException {

		// this is to .perform() of previous operations
		m_endCommandId = m_commandID - 1;
		
		//.perform() of previous operations
		if (m_startCommandId <= m_endCommandId)
			perform();
		
		// Why sort is an action?
		// Because all data in the data partition of all crun(s) should be ready
		// before starting the sort
		
		// TODO sort is an action. Do it immediately and not through DRangeRunner
		// use all crun from crun list, get the cnb of all crun(s) and directly invoke
		// method on the cnb
		
		// Create comparator config for sort action
		m_configNodeType = DataAsyncConfigNode.CNT_COMPARATOR_CONFIG_1_1;
		DMComparatorConfig_1_1 config = new DMComparatorConfig_1_1(unit);
		m_configNode = new DataAsyncConfigNode(config);
		config.setSourceDataId(dataId1);
		config.setTargetDataId(dataId2);

		m_commandMap.put("" + m_commandID, m_configNode);
		m_commandID++;
		
		m_configNodeList.add(m_configNode);
		
		m_configNode = null;
		m_configNodeType = 0;
		
		m_dataId1 = null;
		m_dataId2 = null;
		
		m_startCommandId = m_commandID;
		m_endCommandId = m_commandID;
	
		return this;
	}
	
	public Data input(String jsonStrInput) {
		m_jsonStrInput = jsonStrInput;
		return this;
	}
	
	public Data input(Dson jsonInput) {
		m_jsonStrInput = jsonInput.toString();
		return this;
	}

	public Data input(Properties propertyInput) {
		Dson dson = new Dson();
		Set<String> st = propertyInput.stringPropertyNames();
		for (String s : st) {
			dson = dson.add(s, propertyInput.getProperty(s));
		}
		m_jsonStrInput = dson.toString();
		return this;
	}

	public Data input(HashMap<String, String> mapInput) {
		Dson dson = new Dson();
		Set<String> st = mapInput.keySet();
		for (String s : st) {
			dson = dson.add(s, mapInput.get(s));
			
		}
		m_jsonStrInput = dson.toString();
		return this;
	}
	
	public Data output(HashMap<String, String> outputMap) {
		m_outputMap = outputMap;
		return this;
	}
	
	public Data maxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return this;
	}
	
	public Data maxThreads(long maxThreads) {
		m_maxThreads = maxThreads;
		return this;
	}
		
	public int addToFutureList(Future<?> f) {
		synchronized (this) {
			m_futureList.add(f);
			m_futureListSize++;
		}
		return 0;
	}
	
	/* Not used
	public Future<?> getFromFutureList(long index) throws DScabiException {
		synchronized (this) {
			// Previous works return m_futureList.get(index);
			ListIterator<Future<?>> itr = DMUtil.iteratorBefore(m_futureList, index);
			return itr.next();
		}
	}
	*/
	
	private int getComputeNoBlockMany(long splitTotal) throws /*ParseException,*/ IOException, DScabiException, DScabiClientException {
		
		String jsonString = m_meta.getComputeNoBlockManyJsonStr(splitTotal);
		
		DMJson djson = new DMJson(jsonString);
		long count = djson.getCount();
		Set<String> st = djson.keySet();
		if (0 == count)
			throw new DScabiException("Zero Compute Server available", "CAC.GCN.1");
		for (String s : st) {
			if (s.equals("Count"))
				continue;
			m_cnbList.add(new DataNoBlock(djson.getString(s)));
			m_cnbListSize++;
		}

		return 0;
	}
	
	public void run() {
		
        long k = 0;

		try {
			if (m_splitTotal > 1)
				getComputeNoBlockMany(m_splitTotal);
			else
				getComputeNoBlockMany(m_splitTotal + 1);
		} catch (IOException | DScabiException | DScabiClientException e) {
			// e.printStackTrace();
			throw new RuntimeException(e);
		}
		log.debug("run() m_cnbListSize : {}", m_cnbListSize);
		for (DataNoBlock cnb : m_cnbList) {
        	log.debug("run() Compute is {}", cnb);
        }

		ListIterator<DataNoBlock> itr = m_cnbList.listIterator();
        	
		for (long i = 1; i <= m_splitTotal; i++) {	
    		log.debug("Inside split for loop");
        	DataAsyncRun_D2 crun = new DataAsyncRun_D2(); 
        	m_crunList.add(crun);
        	m_crunListSize++;
        	crun.setCommandMap(m_commandMap);
        	if (DataAsyncConfigNode.CNT_DATAUNIT_CONFIG == m_commandMap.get("1").getConfigNodeType())
        		crun.setConfig(m_commandMap.get("1").getDataUnitConfig());
        	else if (DataAsyncConfigNode.CNT_PARTITIONER_CONFIG == m_commandMap.get("1").getConfigNodeType())
        		crun.setConfig(m_commandMap.get("1").getPartitionerConfig());
        	crun.setTU(m_splitTotal);
        	crun.setSU(i);
        	crun.setMaxRetry(m_maxRetry);
        	crun.setCommandIdRange(m_startCommandId, m_endCommandId);
        	
    		if (itr.hasNext())
            	crun.setComputeNB(itr.next());
    		else {
    			itr = m_cnbList.listIterator();
    			crun.setComputeNB(itr.next());
    		}
        	
     	}
        
        m_crunListReady = true;
        
        long totalCRun = m_crunListSize;
        log.debug("run() totalCRun : {}", totalCRun); 
        
        k = 0;
        long count = 0;
        log.debug("m_noOfRangeRunners : {}", m_noOfRangeRunners);
        for (long i = 0; i < m_noOfRangeRunners; i++) {
        	long startCRun = k;
        	long endCRun = k + (totalCRun / m_noOfRangeRunners);
        	if (totalCRun == m_noOfRangeRunners)
        		endCRun = k;
        	if (endCRun >= totalCRun)
        		endCRun = totalCRun - 1;
        	DataRangeRunner_D2 rr = null;
			try {
				rr = new DataRangeRunner_D2(this, startCRun, endCRun);
				log.debug("DRangeRunner created. startCRun : {}, endCRun : {}", startCRun, endCRun);
				count++;
			} catch (DScabiException e) {
				// e.printStackTrace();
				throw new RuntimeException(e);
			}
     	
        	Future<?> f = m_threadPool.submit(rr);
        	addToFutureList(f);
        	putFutureRRunMap(f, rr);
        	
        	k = endCRun + 1;
        	if (k >= totalCRun) {
        		log.debug("run() DRangeRunner(s) created");
        		break;
        	}
        	// log.debug("run() proceeding");
        }
        m_futureListReady = true;
        
        m_noOfRangeRunners = count;
        log.debug("run() DRangeRunner(s) created count : {}", count);
        m_retryAsyncMonitor = new DataRetryAsyncMonitor_D2(this, m_meta);
        m_futureRetry = m_threadPool.submit(m_retryAsyncMonitor);
               
	}

	public boolean finish() throws DScabiException, ExecutionException, InterruptedException {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("finish() m_splitTotal : {}", m_splitTotal);
		log.debug("finish() m_crunListSize : {}", m_crunListSize);
		log.debug("finish() m_crunListReady : {}", m_crunListReady);
		log.debug("finish() m_futureListReady : {}", m_futureListReady);
		
		try {
			m_futureCompute.get();
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			// e.printStackTrace();
			closeCNBConnections();
			throw e;
		}
		
		log.debug("finish() m_futureListSize : {}", m_futureListSize);
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		log.debug("finish() m_futureListSize : {}", m_futureListSize);

		for (Future<?> f : m_futureList) {
        	
			try {
				f.get();
					
			} catch (CancellationException | InterruptedException | ExecutionException e) {
				// e.printStackTrace();
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
			
				DataRangeRunner_D2 rr = getFutureRRunMap(f);
				ListIterator<DataAsyncRun_D2> itr = DMUtil.iteratorBefore(m_crunList, rr.getStartCRun());
				for (long j = rr.getStartCRun(); j <= rr.getEndCRun(); j++) {
					DataAsyncRun_D2 crun = null;
					if (itr.hasNext())
						crun = itr.next();
					else {
						closeCNBConnections();
						throw new DScabiException("No more crun", "COE.FIH.1");
					}
					
					synchronized(crun) {
					
					IConfig config = crun.getConfig();
					synchronized (config) {
						if (false == config.isResultSet(crun.getSU())) {
							crun.setExecutionError(errorJsonStr);
							// crun.setExecutionError(errorJsonStr) sets crun's m_isDone to true so that 
							// DRetryAsyncMonitor doesn't go into infinite loop
						}
					}
	
					} // End synchronized crun
				} // End for
				
			} // End catch
        
        } // End for
		
		try {
			m_futureRetry.get();
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			// e.printStackTrace();
			closeCNBConnections();
			throw e;
		}
		
		closeCNBConnections();
 		log.debug("finish() Exiting finish()");
 		initialize();
 		return true;
	
	}
	
	private int closeCNBConnections() {
		if (null == m_retryAsyncMonitor)
			return 0;
		List<DataNoBlock> cnba = m_retryAsyncMonitor.getCNBList();
		if (null == cnba)
			return 0;
		for (DataNoBlock cnb : cnba) {
			try {
				cnb.close();
			} catch (Error | RuntimeException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			}
			
		}
		return 0;
		
	}
	
	public boolean finish(long checkTillNanoSec) throws Exception {
		if (false == m_isPerformInProgress)
			return true;
		long time1 = System.nanoTime();
		log.debug("finish(nanosec) m_splitTotal : {}", m_splitTotal);
		log.debug("finish(nanosec) m_crunListSize : {}", m_crunListSize);
		log.debug("finish(nanosec) m_crunListReady : {}", m_crunListReady);
		log.debug("finish(nanosec) m_futureListReady : {}", m_futureListReady);

		try {
			m_futureCompute.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			// e.printStackTrace();
			closeCNBConnections();
			throw e;
		}

		log.debug("finish(nanosec) m_futureListSize : {}", m_futureListSize);
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish(nanosec) m_futureListReady : {}", m_futureListReady);
			gap++; // this is just for the log.debug issue mentioned above
			if (System.nanoTime() - time1 >= checkTillNanoSec)
				return false;

		}
		log.debug("finish(nanosec) m_futureListSize : {}", m_futureListSize);
		
        for (Future<?> f : m_futureList) {
       	
       		try {
				f.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
					
			} catch (CancellationException | InterruptedException | ExecutionException e) {
				// e.printStackTrace();
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
			
				DataRangeRunner_D2 rr = getFutureRRunMap(f);
				ListIterator<DataAsyncRun_D2> itr = DMUtil.iteratorBefore(m_crunList, rr.getStartCRun());
				for (long j = rr.getStartCRun(); j <= rr.getEndCRun(); j++) {
					DataAsyncRun_D2 crun = null;
					if (itr.hasNext())		
						crun = itr.next();
					else {
						closeCNBConnections();
						throw new DScabiException("No more crun", "DCE.FIH2.1");
					}
					
					synchronized(crun) {
					
					IConfig config = crun.getConfig();
					synchronized (config) {
						if (false == config.isResultSet(crun.getSU())) {
							crun.setExecutionError(errorJsonStr);
							// crun.setExecutionError(errorJsonStr) sets crun's m_isDone to true so that 
							// DRetryAsyncMonitor doesn't go into infinite loop
						}
					}
	
					} // End synchronized crun
				} // End for
				
			} // End catch
       
        } // End for
        
		try {
			m_futureRetry.get();
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			// e.printStackTrace();
			closeCNBConnections();
			throw e;
		}

        closeCNBConnections();
		log.debug("finish(nanosec) Exiting finish()");
		initialize();
		return true;

	}

	public boolean isDone() {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("isDone() m_splitTotal : {}", m_splitTotal);
		log.debug("isDone() m_crunListSize : {}", m_crunListSize);
		log.debug("isDone() m_crunListReady : {}", m_crunListReady);
		if (false == m_crunListReady)
			return false;

		boolean check = true;

		check = m_futureCompute.isDone();
		check = m_futureRetry.isDone();
		
		log.debug("isDone() m_futureListSize : {}", m_futureListSize);
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("isDone() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		log.debug("isDone() m_futureListSize : {}", m_futureListSize);
		
		for (Future<?> f : m_futureList) {
  			check = f.isDone();
        }

		log.debug("isDone() Exiting finish()");
		return check;
	}

	
	public Data perform() throws DScabiException, IOException {

		if (false == m_isPerformInProgress)
			m_isPerformInProgress = true;
		else {
			throw new DScabiException("Perform already in progress", "DAA.PEM.1");
		}
		
		if (m_configNode != null) {
			if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_1 == m_configNode.getConfigNodeType()) {
				setForOperatorConfig_1_1(m_configNode.getOperatorConfig_1_1());
			} else if (DataAsyncConfigNode.CNT_OPERATOR_CONFIG_1_2 == m_configNode.getConfigNodeType()) {
				setForOperatorConfig_1_2(m_configNode.getOperatorConfig_1_2());
			}

			m_commandMap.put("" + m_commandID, m_configNode);
			m_commandID++;

			m_configNodeList.add(m_configNode);
			
			m_configNode = null;
			
		}

		if (DIT_DATAUNIT_CLASS == m_dataInitiatorType) {
			setDataUnitClass(m_dataUnitClass);
		} else if (DIT_DATAUNIT_OBJECT == m_dataInitiatorType) {
			setDataUnitObject(m_dataUnitObject);
		} else if (DIT_PARTITIONER_CLASS == m_dataInitiatorType) {
			setPartitionerClass(m_partitionerClass);
		} else if (DIT_PARTITIONER_OBJECT == m_dataInitiatorType) {
			setPartitionerObject(m_partitionerObject);
		} else {
			throw new DScabiException("Data Unit or Partitioner is not set", "Driver.DAA.PEM.1");
		} 
		
		if (DIT_DATAUNIT_CLASS == m_dataInitiatorType) {
			try {
				DataUnit du = (DataUnit) m_dataUnitClass.newInstance(); 
				DataContext dataCtx = DataContext.dummy();
				dataCtx.add("JsonInput", m_jsonStrInput);
				m_splitTotal = du.count(dataCtx);
			} catch (Exception ex) {
				throw new DScabiException(ex.toString(), "Driver.DAA.PEM.1");
			}
		} else if (DIT_DATAUNIT_OBJECT == m_dataInitiatorType) {
			try {
				DataContext dataCtx = DataContext.dummy();
				dataCtx.add("JsonInput", m_jsonStrInput);
				m_splitTotal = m_dataUnitObject.count(dataCtx);
			} catch (Exception ex) {
				throw new DScabiException(ex.toString(), "Driver.DAA.PEM.1");
			}
		} else if (DIT_PARTITIONER_CLASS == m_dataInitiatorType) {
			try {
				DPartitioner p = (DPartitioner) m_partitionerClass.newInstance(); 
				DataContext dataCtx = DataContext.dummy();
				dataCtx.add("JsonInput", m_jsonStrInput);
				m_splitTotal = p.count(dataCtx);
			} catch (Exception ex) {
				throw new DScabiException(ex.toString(), "Driver.DAA.PEM.1");
			}
		} else if (DIT_PARTITIONER_OBJECT == m_dataInitiatorType) {
			try {
				DataContext dataCtx = DataContext.dummy();
				dataCtx.add("JsonInput", m_jsonStrInput);
				m_splitTotal = m_partitionerObject.count(dataCtx);
			} catch (Exception ex) {
				throw new DScabiException(ex.toString(), "Driver.DAA.PEM.1");
			}
		} else {
			throw new DScabiException("Data Unit or Partitioner is not set", "Driver.DAA.PEM.2");
		} 
		
		log.debug("perform() m_splitTotal : {}", m_splitTotal);

		if (null == m_commandMap.get("1")) {
			throw new DScabiException("Data cluster initiator is not set", "Driver.DAA.PEM.1");
		}
    	if (DataAsyncConfigNode.CNT_DATAUNIT_CONFIG != m_commandMap.get("1").getConfigNodeType() &&
    		DataAsyncConfigNode.CNT_PARTITIONER_CONFIG != m_commandMap.get("1").getConfigNodeType())
			throw new DScabiException("Config Type is not Data unit or Partitioner Config for item 1 in Command Map", "Driver.DAA.PEM.3");
    	
		// Previous works String jobId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works jobId = jobId.replace('-', '_');
		String jobId = m_appId + "_" + M_DMCOUNTER.inc();
		
		if (DataAsyncConfigNode.CNT_DATAUNIT_CONFIG == m_commandMap.get("1").getConfigNodeType()) {
			if (m_isJarFilePathListSet) {
				m_commandMap.get("1").getDataUnitConfig().setJarFilePathFromList(m_jarFilePathList);
			}
			if (m_isComputeUnitJarsSet) {
				m_commandMap.get("1").getDataUnitConfig().setComputeUnitJars(m_dcl);
			}
			m_commandMap.get("1").getDataUnitConfig().setAppName(m_appName);
			m_commandMap.get("1").getDataUnitConfig().setAppId(m_appId);
			m_commandMap.get("1").getDataUnitConfig().setJobId(jobId, 1);
			m_commandMap.get("1").getDataUnitConfig().combineJarsForAddJarJsonField();
		} else if (DataAsyncConfigNode.CNT_PARTITIONER_CONFIG == m_commandMap.get("1").getConfigNodeType()) {
			if (m_isJarFilePathListSet) {
				m_commandMap.get("1").getPartitionerConfig().setJarFilePathFromList(m_jarFilePathList);
			}
			if (m_isComputeUnitJarsSet) {
				m_commandMap.get("1").getPartitionerConfig().setComputeUnitJars(m_dcl);
			}
			m_commandMap.get("1").getPartitionerConfig().setAppName(m_appName);
			m_commandMap.get("1").getPartitionerConfig().setAppId(m_appId);
			m_commandMap.get("1").getPartitionerConfig().setJobId(jobId, 1);
			m_commandMap.get("1").getPartitionerConfig().combineJarsForAddJarJsonField();
		} else {
			throw new DScabiException("Config Node entry at 1 in command map is not CNT_DATAUNIT_CONFIG or CNT_PARTITIONER_CONFIG", "DAA.PEM.1");
		}
		
		log.debug("perform() m_maxThreads : {}", m_maxThreads);
		if (0 == m_splitTotal) {
			log.debug("m_splitTotal is zero. Cannot proceed with perform()");
			throw new DScabiException("m_splitTotal is zero. Cannot proceed with perform()", "COE.PEM.2");
		}
		
		m_endCommandId = m_commandID - 1;
		
		if (0 == m_maxThreads) {
			long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			log.debug("perform() usedMemory : {}", usedMemory);
			long freeMemory = Runtime.getRuntime().maxMemory() - usedMemory;
			log.debug("perform() freeMemory : {}", freeMemory);
			
			long noOfThreads = freeMemory / (1024 * 1024); // Assuming 1 Thread consumes 1MB Stack memory
			noOfThreads = Runtime.getRuntime().availableProcessors();
			log.debug("perform() noOfThreads : {}", noOfThreads);
			
			if (m_splitTotal + 1 < noOfThreads) {
				if (m_splitTotal + 1 < Integer.MAX_VALUE) {
					m_threadPool = Executors.newFixedThreadPool((int)(m_splitTotal + 1)); // +1 for retry thread, +1 to include thread for this class run() method
					log.debug("threads created : {}", m_splitTotal + 1);
					m_noOfRangeRunners = m_splitTotal;
				}
				else if (noOfThreads < Integer.MAX_VALUE) {
					m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
					log.debug("threads created : {}", noOfThreads);
					if (noOfThreads > 1)
						m_noOfRangeRunners = noOfThreads - 1;
					else
						m_noOfRangeRunners = noOfThreads;
				}
				else {
					m_threadPool = Executors.newFixedThreadPool(Integer.MAX_VALUE); // +1 to include thread for this class run() method
					log.debug("threads created : {}", Integer.MAX_VALUE);
					m_noOfRangeRunners = Integer.MAX_VALUE - 1;
				}
				//m_threadPool = new DThreadPoolExecutor(
				//		m_splitTotal + 2, m_splitTotal + 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				
			} else {
				if (noOfThreads < Integer.MAX_VALUE) {
					m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
					log.debug("threads created : {}", noOfThreads);
					if (noOfThreads > 1)
						m_noOfRangeRunners = noOfThreads - 1;
					else
						m_noOfRangeRunners = noOfThreads;
				}	
				else {
					m_threadPool = Executors.newFixedThreadPool(Integer.MAX_VALUE);
					log.debug("threads created : {}", Integer.MAX_VALUE);
					m_noOfRangeRunners = Integer.MAX_VALUE - 1;
				}
				//m_threadPool = new DThreadPoolExecutor(
				//		(int)noOfThreads, (int)noOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
			}
		}
		else
		{
			if (m_maxThreads < Integer.MAX_VALUE) {
				m_threadPool = Executors.newFixedThreadPool((int)m_maxThreads);
				log.debug("perform() threads created : {}", m_maxThreads);
				if (m_maxThreads > 1)
					m_noOfRangeRunners = m_maxThreads - 1;
				else
					m_noOfRangeRunners = m_maxThreads;
			}
			else {
				m_threadPool = Executors.newFixedThreadPool(Integer.MAX_VALUE);
				log.debug("threads created : {}", Integer.MAX_VALUE);
				m_noOfRangeRunners = Integer.MAX_VALUE - 1;
			}
			//m_threadPool = new DThreadPoolExecutor(
			//		m_maxThreads, m_maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
		}
		log.debug("m_noOfRangeRunners : {}", m_noOfRangeRunners);
        log.debug("m_startCommandId : {}", m_startCommandId);
        log.debug("m_endCommandId : {}", m_endCommandId);
        
		m_futureCompute = m_threadPool.submit(this);
		
		return this;
	}
	
}
