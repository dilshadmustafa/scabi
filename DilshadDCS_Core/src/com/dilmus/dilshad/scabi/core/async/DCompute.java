/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 29-Feb-2016
 * File Name : DCompute.java
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

5. You should not redistribute any modified source code of this Software and/or its 
compiled object binary form with any changes, additions, enhancements, updates or 
modifications, any modified works of this Software, any straight forward translation 
and/or implementation to same and/or another programming language and embedded modified 
versions of this Software source code and/or its compiled object binary in any form, 
both within as well as outside your organization, company, legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. Under differently named or renamed software, you should not redistribute this 
Software and/or any modified works of this Software, including its source code 
and/or its compiled object binary form. Under your name or your company name or 
your product name, you should not publish this Software, including its source code 
and/or its compiled object binary form, modified or original. 

8. You agree to use the original source code from Dilshad Mustafa's project only
and/or the compiled object binary form of the original source code.

9. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

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

package com.dilmus.dilshad.scabi.core.async;

import java.io.IOException;
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
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.Dson;

/**
 * @author Dilshad Mustafa
 *
 */
public class DCompute implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DCompute.class);
	private ExecutorService m_threadPool = null;
	private DMeta m_meta = null;
	private int m_commandID = 1;
	private HashMap<String, DComputeAsyncConfig> m_commandMap = null;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private long m_maxSplit = 1;
	private int m_maxRetry = 3; //0;
	private long m_maxThreads = 1;
	private long m_splitTotal = 0;
	private DComputeAsyncConfig m_config = null;
	private boolean m_isPerformInProgress = false;
	private boolean m_crunListReady = false;
	private boolean m_futureListReady = false;

	private LinkedList<DComputeAsyncConfig> m_cconfigList = null;
	private LinkedList<DComputeAsyncRun> m_crunList = null;
	private LinkedList<DComputeNoBlock> m_cnbList = null;
	
	private LinkedList<Future<?>> m_futureList = null;
	private HashMap<Future<?>, DRangeRunner> m_futureRRunMap = null;
	
	private Future<?> m_futureCompute = null;
	private Future<?> m_futureRetry = null;
	
	private long m_noOfRangeRunners = 0;
	
	private DRetryAsyncMonitor m_retryAsyncMonitor = null;
	
	private boolean m_isSplitSet = false;
	private long m_startSplit = -1;
	private long m_endSplit = -1;
	
	private boolean m_isJarFilePathListSet = false;
	private LinkedList<String> m_jarFilePathList = null;
	
	private String m_emptyJsonStr = Dson.empty();
	
	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	private long m_crunListSize = 0;
	private long m_cnbListSize = 0;
	private long m_futureListSize = 0;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	public LinkedList<DComputeAsyncConfig> getCConfigList() {
		return m_cconfigList;
	}
	
	public LinkedList<DComputeAsyncRun> getCRunList() {
		return m_crunList;
	}
	
	public LinkedList<DComputeNoBlock> getCNBList() {
		return m_cnbList;
	}
	
	public ExecutorService getExecutorService() {
		return m_threadPool;
	}
	
	public int putFutureRRunMap(Future<?> f, DRangeRunner rrun) {
		synchronized (this) {
			m_futureRRunMap.put(f, rrun);
		}
		return 0;
	}
	
	public DRangeRunner getFutureRRunMap(Future<?> f) {
		synchronized (this) {
			return m_futureRRunMap.get(f);
		}
	}

	public HashMap<Future<?>, DRangeRunner> getFutureRRunMap() {
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
	
	public DCompute(DMeta meta) {
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DComputeAsyncConfig>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 1;
		m_splitTotal = 0;
		
		m_cconfigList = new LinkedList<DComputeAsyncConfig>();
		m_crunList = new LinkedList<DComputeAsyncRun>();
		m_cnbList = new LinkedList<DComputeNoBlock>();
		
		m_futureList = new LinkedList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DRangeRunner>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cnbListSize = 0;
		m_futureListSize = 0;
	}
	
	public int initialize() throws InterruptedException {
		
		m_threadPool.shutdownNow();
		m_threadPool.awaitTermination(10, TimeUnit.MINUTES);
		m_threadPool = null;

		m_commandID = 1;
		m_commandMap.clear();
		m_jsonStrInput = m_emptyJsonStr;
		m_outputMap = null;
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 1;
		m_splitTotal = 0;
		
		m_config = null;
		
		m_isSplitSet = false;
		m_startSplit = -1;
		m_endSplit = -1;

		m_isPerformInProgress = false;
		m_crunListReady = false;
		m_futureListReady = false;

		synchronized (this) {
			
			m_cconfigList.clear();
			// m_crunList is not thread safe and RetryMonitor is also using it
			// So it is cleared after thread pool shutdown
			m_crunList.clear();
			m_cnbList.clear();
					
			m_futureList.clear();
			m_futureRRunMap.clear();
			
			m_isJarFilePathListSet = false;
			m_jarFilePathList.clear();
			
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
			
			m_crunListSize = 0;
			m_cnbListSize = 0;
			m_futureListSize = 0;
		}
		
		return 0;
	}
	
	public int close() {
		m_threadPool.shutdownNow();
		m_threadPool = null;
		return 0;
	}
	
	public long getCRunListSize() {
		return m_crunListSize;
	}
	
	public long getCNBListSize() {
		return m_cnbListSize;
	}

	public DCompute executeCode(String code) throws DScabiException {
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "COE.ECE.1");
		}
		if (m_config != null) {
			m_config.setInput(m_jsonStrInput);
			m_config.setOutput(m_outputMap);
			m_config.setMaxSplit(m_maxSplit);
			m_config.setMaxRetry(m_maxRetry);
			
			if (m_isSplitSet) {
				if (m_startSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "CAS.ECE.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "CAS.ECE.3");
				m_config.setSplitRange(m_startSplit, m_endSplit);
			}

			m_commandMap.put("" + m_commandID, m_config);
			m_commandID++;
			
			if (m_isSplitSet)
				m_splitTotal = m_splitTotal + (m_endSplit - m_startSplit + 1);
			else
				m_splitTotal = m_splitTotal + m_maxSplit;

			if (m_isJarFilePathListSet) {
				m_config.setJarFilePathFromList(m_jarFilePathList);
				m_isJarFilePathListSet = false;
				m_jarFilePathList.clear();
			}
			
			if (m_isComputeUnitJarsSet) {
				m_config.setComputeUnitJars(m_dcl);
				m_isComputeUnitJarsSet = false;
				m_dcl = null;
			}

			m_maxSplit = 1;
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeAsyncConfig(code);
		
		return this;
	}

	public DCompute executeClass(Class<? extends DComputeUnit> cls) throws DScabiException {
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "COE.EOT.1");
		}
		if (m_config != null) {
			m_config.setInput(m_jsonStrInput);
			m_config.setOutput(m_outputMap);
			m_config.setMaxSplit(m_maxSplit);
			m_config.setMaxRetry(m_maxRetry);
			
			if (m_isSplitSet) {
				if (m_startSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "CAS.EOT.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "CAS.EOT.3");
				m_config.setSplitRange(m_startSplit, m_endSplit);
			}

			m_commandMap.put("" + m_commandID, m_config);
			m_commandID++;
			
			if (m_isSplitSet)
				m_splitTotal = m_splitTotal + (m_endSplit - m_startSplit + 1);
			else
				m_splitTotal = m_splitTotal + m_maxSplit;

			if (m_isJarFilePathListSet) {
				m_config.setJarFilePathFromList(m_jarFilePathList);
				m_isJarFilePathListSet = false;
				m_jarFilePathList.clear();
			}

			if (m_isComputeUnitJarsSet) {
				m_config.setComputeUnitJars(m_dcl);
				m_isComputeUnitJarsSet = false;
				m_dcl = null;
			}
			
			m_maxSplit = 1;
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeAsyncConfig(cls);
		
		return this;
	}

	
	public DCompute executeObject(DComputeUnit unit) throws DScabiException {
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "COE.EOT.1");
		}
		if (m_config != null) {
			m_config.setInput(m_jsonStrInput);
			m_config.setOutput(m_outputMap);
			m_config.setMaxSplit(m_maxSplit);
			m_config.setMaxRetry(m_maxRetry);
			
			if (m_isSplitSet) {
				if (m_startSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "CAS.EOT.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "CAS.EOT.3");
				m_config.setSplitRange(m_startSplit, m_endSplit);
			}
			
			m_commandMap.put("" + m_commandID, m_config);
			m_commandID++;
			
			if (m_isSplitSet)
				m_splitTotal = m_splitTotal + (m_endSplit - m_startSplit + 1);
			else
				m_splitTotal = m_splitTotal + m_maxSplit;

			if (m_isJarFilePathListSet) {
				m_config.setJarFilePathFromList(m_jarFilePathList);
				m_isJarFilePathListSet = false;
				m_jarFilePathList.clear();
			}

			if (m_isComputeUnitJarsSet) {
				m_config.setComputeUnitJars(m_dcl);
				m_isComputeUnitJarsSet = false;
				m_dcl = null;
			}

			m_maxSplit = 1;
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeAsyncConfig(unit);
		
		return this;
	}

	public DCompute executeJar(String jarFilePath, String classNameInJar) throws DScabiException {
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "COE.ECN.1");
		}
		if (m_config != null) {
			m_config.setInput(m_jsonStrInput);
			m_config.setOutput(m_outputMap);
			m_config.setMaxSplit(m_maxSplit);
			m_config.setMaxRetry(m_maxRetry);
			
			if (m_isSplitSet) {
				if (m_startSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "CAS.ECN.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "CAS.ECN.3");
				m_config.setSplitRange(m_startSplit, m_endSplit);
			}
			
			m_commandMap.put("" + m_commandID, m_config);
			m_commandID++;
			
			if (m_isSplitSet)
				m_splitTotal = m_splitTotal + (m_endSplit - m_startSplit + 1);
			else
				m_splitTotal = m_splitTotal + m_maxSplit;

			if (m_isJarFilePathListSet) {
				m_config.setJarFilePathFromList(m_jarFilePathList);
				m_isJarFilePathListSet = false;
				m_jarFilePathList.clear();
			}

			if (m_isComputeUnitJarsSet) {
				m_config.setComputeUnitJars(m_dcl);
				m_isComputeUnitJarsSet = false;
				m_dcl = null;
			}

			m_maxSplit = 1;
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeAsyncConfig(jarFilePath, classNameInJar);
		
		return this;
	}

	
	public DCompute input(String jsonStrInput) {
		m_jsonStrInput = jsonStrInput;
		return this;
	}
	
	public DCompute input(Dson jsonInput) {
		m_jsonStrInput = jsonInput.toString();
		return this;
	}

	public DCompute input(Properties propertyInput) {
		Dson dson = new Dson();
		Set<String> st = propertyInput.stringPropertyNames();
		for (String s : st) {
			dson = dson.add(s, propertyInput.getProperty(s));
		}
		m_jsonStrInput = dson.toString();
		return this;
	}

	public DCompute input(HashMap<String, String> mapInput) {
		Dson dson = new Dson();
		Set<String> st = mapInput.keySet();
		for (String s : st) {
			dson = dson.add(s, mapInput.get(s));
			
		}
		m_jsonStrInput = dson.toString();
		return this;
	}
	
	public DCompute output(HashMap<String, String> outputMap) {
		m_outputMap = outputMap;
		return this;
	}

	public DCompute split(long maxSplit) throws DScabiException {
		if (maxSplit <= 0)
			throw new DScabiException("Split should not be <= 0" , "CAS.SPT.1");
		m_maxSplit = maxSplit;
		return this;
	}
	
	public DCompute splitRange(long startSplit, long endSplit) throws DScabiException {
		
		if (startSplit <= 0) {
			throw new DScabiException("startSplit should not be <= 0", "CAS.SRE.1");
		}
		if (endSplit <= 0) {
			throw new DScabiException("endSplit should not be <= 0", "CAS.SRE.2");
		}
		if (startSplit > endSplit) {
			throw new DScabiException("startSplit should not be > endSplit", "CAS.SRE.3");
		}
		m_isSplitSet = true;
		m_startSplit = startSplit;
		m_endSplit = endSplit;

		return this;
	}

	
	public DCompute maxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return this;
	}
	
	public DCompute maxThreads(long maxThreads) {
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
			m_cnbList.add(new DComputeNoBlock(djson.getString(s)));
			m_cnbListSize++;
		}

		return 0;
	}
	
	public void run() {
		
		//Previous works LinkedList<DComputeNoBlock> cnba = null;
        long k = 0;

		try {
			/* Previous works
			cnba = m_meta.getComputeNoBlockMany(m_splitTotal);
			if (null == cnba)
				throw new DScabiClientException("Zero Compute Server available", "CAC.RUN.1");
			*/
			if (m_splitTotal > 1)
				getComputeNoBlockMany(m_splitTotal);
			else
				getComputeNoBlockMany(m_splitTotal + 1);
		} catch (/*ParseException |*/ IOException | DScabiException | DScabiClientException e) {
			//e.printStackTrace();
			throw new RuntimeException(e);
		}
		/* Previous works
		log.debug("run() DMUtil.listSize(cnba) : {}", DMUtil.listSize(cnba));
		for (DComputeNoBlock cnb : cnba) {
        	log.debug("run() Compute is {}", cnb);
        	m_cnbList.add(cnb);
        }
		*/
		log.debug("run() m_cnbListSize : {}", m_cnbListSize);
		for (DComputeNoBlock cnb : m_cnbList) {
        	log.debug("run() Compute is {}", cnb);
        }

        Set<String> st = m_commandMap.keySet();
        for (String key : st) {
        	DComputeAsyncConfig config = m_commandMap.get(key);
			long maxSplit = config.getMaxSplit();
			log.debug("run() maxSplit for this config : {}", maxSplit);
        	long startSplit = 1;
        	long endSplit = maxSplit;
			if (config.isSplitSet()) {
				startSplit = config.getStartSplit();
				log.debug("run() startSplit : {}", startSplit);
				endSplit = config.getEndSplit();
				log.debug("run() endSplit : {}", endSplit);
			}
        	
			ListIterator<DComputeNoBlock> itr = m_cnbList.listIterator();
			for (long i = startSplit; i <= endSplit; i++) {	
        		log.debug("Inside split for loop");
        		//Previous works if (k >= cnba.size())
    	        //Previous works		k = 0;
            	DComputeAsyncRun crun = new DComputeAsyncRun(); 
            	m_crunList.add(crun);
            	m_crunListSize++;
            	crun.setConfig(config);
            	crun.setTU(maxSplit);
            	crun.setSU(i);
            	crun.setMaxRetry(config.getMaxRetry());
            	//Previous works crun.setComputeNB(cnba.get(k));
        		if (itr.hasNext())
                	crun.setComputeNB(itr.next());
        		else {
        			itr = m_cnbList.listIterator();
        			crun.setComputeNB(itr.next());
        		}
            	//Previous works k++;
         	}
        }
        //Previous works log.debug("run() m_crunList.size() : {}", m_crunList.size()); 
        
        m_crunListReady = true;
        
        //Previous works int totalCRun = m_crunList.size();
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
        	DRangeRunner rr = null;
			try {
				rr = new DRangeRunner(this, startCRun, endCRun);
				log.debug("DRangeRunner created. startCRun : {}, endCRun : {}", startCRun, endCRun);
				count++;
			} catch (DScabiException e) {
				//e.printStackTrace();
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
        	//log.debug("run() proceeding");
        }
        m_futureListReady = true;
        
        m_noOfRangeRunners = count;
        log.debug("run() DRangeRunner(s) created count : {}", count);
        m_retryAsyncMonitor = new DRetryAsyncMonitor(this, m_meta);
        m_futureRetry = m_threadPool.submit(m_retryAsyncMonitor);
               
	}

	public boolean finish() throws DScabiException, ExecutionException, InterruptedException {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("finish() m_splitTotal : {}", m_splitTotal);
		//Previous works log.debug("finish() m_crunList.size() : {}", m_crunList.size());
		log.debug("finish() m_crunListSize : {}", m_crunListSize);
		log.debug("finish() m_crunListReady : {}", m_crunListReady);
		log.debug("finish() m_futureListReady : {}", m_futureListReady);
		
		try {
			m_futureCompute.get();
			m_futureRetry.get();
			
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCNBConnections();
			throw e;
		}
		
		//Previous works log.debug("finish() m_futureList.size() : {}", m_futureList.size());
		log.debug("finish() m_futureListSize : {}", m_futureListSize);
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		//Previous works log.debug("finish() m_futureList.size() : {}", m_futureList.size());
		log.debug("finish() m_futureListSize : {}", m_futureListSize);

		for (Future<?> f : m_futureList) {
        	
			try {
				f.get();
					
			} catch (CancellationException | InterruptedException | ExecutionException e) {
				//e.printStackTrace();
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
			
				DRangeRunner rr = getFutureRRunMap(f);
				ListIterator<DComputeAsyncRun> itr = DMUtil.iteratorBefore(m_crunList, rr.getStartCRun());
				for (long j = rr.getStartCRun(); j <= rr.getEndCRun(); j++) {
					//Previous works DComputeAsyncRun crun = m_crunList.get(j);
					DComputeAsyncRun crun = null;
					if (itr.hasNext())
						crun = itr.next();
					else {
						closeCNBConnections();
						throw new DScabiException("No more crun", "COE.FIH.1");
					}
					DComputeAsyncConfig config = crun.getConfig();
					synchronized (config) {
						if (false == config.isResultSet(crun.getSU())) {
							crun.setExecutionError(errorJsonStr);
						}
					}
	
				} // End for
				
			} // End catch
        
        } // End for
		
		closeCNBConnections();
 		log.debug("finish() Exiting finish()");
 		initialize();
 		return true;
	
	}
	
	private int closeCNBConnections() {
		if (null == m_retryAsyncMonitor)
			return 0;
		List<DComputeNoBlock> cnba = m_retryAsyncMonitor.getCNBList();
		if (null == cnba)
			return 0;
		for (DComputeNoBlock cnb : cnba) {
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
		//Previous works log.debug("finish(nanosec) m_crunList.size() : {}", m_crunList.size());
		log.debug("finish(nanosec) m_crunListSize : {}", m_crunListSize);
		log.debug("finish(nanosec) m_crunListReady : {}", m_crunListReady);
		log.debug("finish(nanosec) m_futureListReady : {}", m_futureListReady);

		try {
			m_futureCompute.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
			m_futureRetry.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
			
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCNBConnections();
			throw e;
		}

		//Previous works log.debug("finish(nanosec) m_futureList.size() : {}", m_futureList.size());
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
		// Previous works log.debug("finish(nanosec) m_futureList.size() : {}", m_futureList.size());
		log.debug("finish(nanosec) m_futureListSize : {}", m_futureListSize);
		
        for (Future<?> f : m_futureList) {
       	
       		try {
				f.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
					
			} catch (CancellationException | InterruptedException | ExecutionException e) {
				//e.printStackTrace();
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
			
				DRangeRunner rr = getFutureRRunMap(f);
				ListIterator<DComputeAsyncRun> itr = DMUtil.iteratorBefore(m_crunList, rr.getStartCRun());
				for (long j = rr.getStartCRun(); j <= rr.getEndCRun(); j++) {
					//Previous works DComputeAsyncRun crun = m_crunList.get(j);
					DComputeAsyncRun crun = null;
					if (itr.hasNext())		
						crun = itr.next();
					else {
						closeCNBConnections();
						throw new DScabiException("No more crun", "DCE.FIH2.1");
					}
					DComputeAsyncConfig config = crun.getConfig();
					synchronized (config) {
						if (false == config.isResultSet(crun.getSU())) {
							crun.setExecutionError(errorJsonStr);
						}
					}
	
				} // End for
				
			} // End catch
       
        } // End for
        closeCNBConnections();
		log.debug("finish(nanosec) Exiting finish()");
		initialize();
		return true;

	}

	public boolean isDone() {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("isDone() m_splitTotal : {}", m_splitTotal);
		//Previous works log.debug("isDone() m_crunList.size() : {}", m_crunList.size());
		log.debug("isDone() m_crunListSize : {}", m_crunListSize);
		log.debug("isDone() m_crunListReady : {}", m_crunListReady);
		if (false == m_crunListReady)
			return false;

		boolean check = true;

		check = m_futureCompute.isDone();
		check = m_futureRetry.isDone();
		
		//Previous works log.debug("isDone() m_futureList.size() : {}", m_futureList.size());
		log.debug("isDone() m_futureListSize : {}", m_futureListSize);
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("isDone() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		//Previous works log.debug("isDone() m_futureList.size() : {}", m_futureList.size());
		log.debug("isDone() m_futureListSize : {}", m_futureListSize);
		
		for (Future<?> f : m_futureList) {
  			check = f.isDone();
        }

		log.debug("isDone() Exiting finish()");
		return check;
	}

	
	public DCompute perform() throws DScabiException {
		if (m_config != null) {
			m_config.setInput(m_jsonStrInput);
			m_config.setOutput(m_outputMap);
			m_config.setMaxSplit(m_maxSplit);
			m_config.setMaxRetry(m_maxRetry);
			
			if (m_isSplitSet) {
				if (m_startSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "CAS.EOT.1");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "CAS.EOT.2");
				m_config.setSplitRange(m_startSplit, m_endSplit);
			}

			m_commandMap.put("" + m_commandID, m_config);
			m_commandID++;
						
			if (m_isSplitSet)
				m_splitTotal = m_splitTotal + (m_endSplit - m_startSplit + 1);
			else
				m_splitTotal = m_splitTotal + m_maxSplit;

			if (m_isJarFilePathListSet) {
				m_config.setJarFilePathFromList(m_jarFilePathList);
				m_isJarFilePathListSet = false;
				m_jarFilePathList.clear();
			}

			if (m_isComputeUnitJarsSet) {
				m_config.setComputeUnitJars(m_dcl);
				m_isComputeUnitJarsSet = false;
				m_dcl = null;
			}

			m_maxSplit = 1;
			m_maxRetry = 0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}

		if (1 == m_commandID) {
			log.debug("No commands are added");
			throw new DScabiException("No commands are added", "COE.PEM.3");
			//return this;
		}
		log.debug("perform() m_splitTotal : {}", m_splitTotal);
		log.debug("perform() m_maxThreads : {}", m_maxThreads);
		if (0 == m_splitTotal) {
			log.debug("m_splitTotal is zero. Cannot proceed with perform()");
			throw new DScabiException("m_splitTotal is zero. Cannot proceed with perform()", "COE.PEM.2");
			//return this;
		}
		
		if (false == m_isPerformInProgress)
			m_isPerformInProgress = true;
		else {
			throw new DScabiException("Perform already in progress", "COE.PEM.1");
		}
		
		String jobId = UUID.randomUUID().toString() + "-" + System.nanoTime() + "-" + M_DMCOUNTER.inc();
		for (DComputeAsyncConfig config : m_cconfigList) {
			config.setJobId(jobId);
		}
		
		if (1 == m_maxThreads) {
			long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			log.debug("perform() usedMemory : {}", usedMemory);
			long freeMemory = Runtime.getRuntime().maxMemory() - usedMemory;
			log.debug("perform() freeMemory : {}", freeMemory);
			
			long noOfThreads = freeMemory / (1024 * 1024); // Assuming 1 Thread consumes 1MB Stack memory
			log.debug("perform() noOfThreads : {}", noOfThreads);
			
			if (m_splitTotal < noOfThreads) {
				if (m_splitTotal + 2 < Integer.MAX_VALUE) {
					m_threadPool = Executors.newFixedThreadPool((int)(m_splitTotal + 2)); // +1 to include thread for this class run() method
					log.debug("threads created : {}", m_splitTotal + 2);
					m_noOfRangeRunners = m_splitTotal;
				}
				else if (noOfThreads < Integer.MAX_VALUE) {
					m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
					log.debug("threads created : {}", noOfThreads);
					m_noOfRangeRunners = (int)noOfThreads - 2;
				}
				else {
					m_threadPool = Executors.newFixedThreadPool(Integer.MAX_VALUE); // +1 to include thread for this class run() method
					log.debug("threads created : {}", Integer.MAX_VALUE);
					m_noOfRangeRunners = Integer.MAX_VALUE - 2;
				}
				//m_threadPool = new DThreadPoolExecutor(
				//		m_splitTotal + 2, m_splitTotal + 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				
			} else {
				if (noOfThreads < Integer.MAX_VALUE) {
					m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
					log.debug("threads created : {}", noOfThreads);
					m_noOfRangeRunners = (int)noOfThreads - 2;
				}	
				else {
					m_threadPool = Executors.newFixedThreadPool(Integer.MAX_VALUE);
					log.debug("threads created : {}", Integer.MAX_VALUE);
					m_noOfRangeRunners = Integer.MAX_VALUE - 2;
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
				if (m_maxThreads > 2)
					m_noOfRangeRunners = m_maxThreads - 2;
				else
					m_noOfRangeRunners = m_maxThreads;
			}
			else {
				m_threadPool = Executors.newFixedThreadPool(Integer.MAX_VALUE);
				log.debug("threads created : {}", Integer.MAX_VALUE);
				m_noOfRangeRunners = Integer.MAX_VALUE - 2;
			}
			//m_threadPool = new DThreadPoolExecutor(
			//		m_maxThreads, m_maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
		}
		log.debug("m_noOfRangeRunners : {}", m_noOfRangeRunners);
        		
		m_futureCompute = m_threadPool.submit(this);
		
		return this;
	}
	
}