/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 29-Feb-2016
 * File Name : DComputeAsync.java
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
to another programming language and embedded modified versions of this Software source 
code and/or its compiled object binary in any form, both within as well as outside your 
organization, company, legal entity and/or individual. 

6. You should not embed any modification of this Software source code and/or its compiled 
object binary form in any way, either partially or fully.

7. You should not redistribute this Software, including its source code and/or its 
compiled object binary form, under differently named or renamed software. You should 
not publish this Software, including its source code and/or its compiled object binary 
form, modified or original, under your name or your company name or your product name. 
You should not sell this Software to any party, organization, company, legal entity 
and/or individual.

8. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

9. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

10. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
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
public class DComputeAsync implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DComputeAsync.class);
	private ExecutorService m_threadPool = null;
	private DMeta m_meta = null;
	private int m_commandID = 1;
	private HashMap<String, DComputeAsyncConfig> m_commandMap = null;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private int m_maxSplit = 1;
	private int m_maxRetry = 0;
	private int m_maxThreads = 1;
	private int m_splitTotal = 0;
	private DComputeAsyncConfig m_config = null;
	private boolean m_isPerformInProgress = false;
	private boolean m_crunListReady = false;
	private boolean m_futureListReady = false;

	private List<DComputeAsyncConfig> m_cconfigList = null;
	private List<DComputeAsyncRun> m_crunList = null;
	private List<DComputeNoBlock> m_cnbList = null;
	
	private List<Future<?>> m_futureList = null;
	private HashMap<Future<?>, DRangeRunner> m_futureRRunMap = null;
	
	private Future<?> m_futureCompute = null;
	private Future<?> m_futureRetry = null;
	
	private int m_noOfRangeRunners = 0;
	
	private DRetryAsyncMonitor m_retryAsyncMonitor = null;
	
	private boolean m_isSplitSet = false;
	private int m_startSplit = -1;
	private int m_endSplit = -1;
	
	private boolean m_isJarFilePathListSet = false;
	private List<String> m_jarFilePathList = null;
	
	private String m_emptyJsonStr = Dson.empty();
	
	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	public List<DComputeAsyncConfig> getCConfigList() {
		return m_cconfigList;
	}
	
	public List<DComputeAsyncRun> getCRunList() {
		return m_crunList;
	}
	
	public List<DComputeNoBlock> getCNBList() {
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
	
	public DComputeAsync(DMeta meta) {
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DComputeAsyncConfig>();
		
		m_maxSplit = 1;
		m_maxRetry = 0;
		m_maxThreads = 1;
		m_splitTotal = 0;
		
		m_cconfigList = new ArrayList<DComputeAsyncConfig>();
		m_crunList = new ArrayList<DComputeAsyncRun>();
		m_cnbList = new ArrayList<DComputeNoBlock>();
		
		m_futureList = new ArrayList<Future<?>>();
		m_futureRRunMap = new HashMap<Future<?>, DRangeRunner>();
		
		m_jarFilePathList = new ArrayList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
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
		m_maxRetry = 0;
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
		
		}
		
		return 0;
	}
	
	public int close() {
		m_threadPool.shutdownNow();
		m_threadPool = null;
		return 0;
	}
	
	public DComputeAsync executeCode(String code) throws DScabiException {
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
			m_maxRetry = 0;

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

	public DComputeAsync executeClass(Class<? extends DComputeUnit> cls) throws DScabiException {
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
			m_maxRetry = 0;

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

	
	public DComputeAsync executeObject(DComputeUnit unit) throws DScabiException {
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
			m_maxRetry = 0;

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

	public DComputeAsync executeJar(String jarFilePath, String classNameInJar) throws DScabiException {
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
			m_maxRetry = 0;

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

	
	public DComputeAsync input(String jsonStrInput) {
		m_jsonStrInput = jsonStrInput;
		return this;
	}
	
	public DComputeAsync input(Dson jsonInput) {
		m_jsonStrInput = jsonInput.toString();
		return this;
	}

	public DComputeAsync input(Properties propertyInput) {
		Dson dson = new Dson();
		Set<String> st = propertyInput.stringPropertyNames();
		for (String s : st) {
			dson = dson.add(s, propertyInput.getProperty(s));
		}
		m_jsonStrInput = dson.toString();
		return this;
	}

	public DComputeAsync input(HashMap<String, String> mapInput) {
		Dson dson = new Dson();
		Set<String> st = mapInput.keySet();
		for (String s : st) {
			dson = dson.add(s, mapInput.get(s));
			
		}
		m_jsonStrInput = dson.toString();
		return this;
	}
	
	public DComputeAsync output(HashMap<String, String> outputMap) {
		// outputTo()
		m_outputMap = outputMap;
		return this;
	}

	public DComputeAsync split(int maxSplit) throws DScabiException {
		if (maxSplit <= 0)
			throw new DScabiException("Split should not be <= 0" , "CAS.SPT.1");
		m_maxSplit = maxSplit;
		return this;
	}
	
	public DComputeAsync splitRange(int startSplit, int endSplit) throws DScabiException {
		
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

	
	public DComputeAsync maxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return this;
	}
	
	public DComputeAsync maxThreads(int maxThreads) {
		m_maxThreads = maxThreads;
		return this;
	}
		
	public int addToFutureList(Future<?> f) {
		synchronized (this) {
			m_futureList.add(f);
		}
		return 0;
	}
	
	public Future<?> getFromFutureList(int index) {
		synchronized (this) {
			return m_futureList.get(index);
		}
	}
	
	public void run() {
		
		//works DComputeNoBlock cnba[];
		List<DComputeNoBlock> cnba = null;
		try {
			cnba = m_meta.getComputeNoBlockMany(m_splitTotal);
			if (null == cnba)
				throw new DScabiClientException("Zero Compute Server available", "CAC.RUN.1");
			//cnba = m_meta.getComputeNoBlockMany(1);
		} catch (ParseException | IOException | DScabiClientException e) {
			//e.printStackTrace();
			throw new RuntimeException(e);
		}
		
        for (DComputeNoBlock cnb : cnba) {
        	log.debug("run() Compute is {}", cnb);
        	m_cnbList.add(cnb);
        }
        
        Set<String> st = m_commandMap.keySet();
        int k = 0;
        for (String key : st) {
        	DComputeAsyncConfig config = m_commandMap.get(key);
			int maxSplit = config.getMaxSplit();
			log.debug("run() maxSplit for this config : {}", maxSplit);
        	int startSplit = 1;
        	int endSplit = maxSplit;
			if (config.isSplitSet()) {
				startSplit = config.getStartSplit();
				log.debug("run() startSplit : {}", startSplit);
				endSplit = config.getEndSplit();
				log.debug("run() endSplit : {}", endSplit);
			}
        	
			//works for (int i = 0; i < maxSplit; i++) {
			for (int i = startSplit; i <= endSplit; i++) {	
        		log.debug("Inside split for loop");
        		//works if (k >= cnba.length)
    	        //works 	k = 0;
        		if (k >= cnba.size())
    	        	k = 0;
            	DComputeAsyncRun crun = new DComputeAsyncRun(); 
            	m_crunList.add(crun);
            	crun.setConfig(config);
            	crun.setTU(maxSplit);
            	//works crun.setSU(i + 1);
            	crun.setSU(i);
            	crun.setMaxRetry(config.getMaxRetry());
            	//works crun.setComputeNB(cnba[k]);
            	crun.setComputeNB(cnba.get(k));
    			k++;
         	}
        }
        log.debug("run() m_crunList.size() : {}", m_crunList.size()); 
        m_crunListReady = true;
        
        int totalCRun = m_crunList.size();
        k = 0;
        int count = 0;
        log.debug("m_noOfRangeRunners : {}", m_noOfRangeRunners);
        for (int i = 0; i < m_noOfRangeRunners; i++) {
        	int startCRun = k;
        	int endCRun = k + (totalCRun / m_noOfRangeRunners);
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
		log.debug("finish() m_crunList.size() : {}", m_crunList.size());
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
		
		log.debug("finish() m_futureList.size() : {}", m_futureList.size());
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		log.debug("finish() m_futureList.size() : {}", m_futureList.size());

		for (Future<?> f : m_futureList) {
			String result = null;
        	
			try {
				f.get();
					
			} catch (CancellationException | InterruptedException | ExecutionException e) {
				//e.printStackTrace();
				result = DMJson.error(DMUtil.clientErrMsg(e));
			
				DRangeRunner rr = getFutureRRunMap(f);
				for (int j = rr.getStartCRun(); j <= rr.getEndCRun(); j++) {
					DComputeAsyncRun crun = m_crunList.get(j);
					DComputeAsyncConfig config = crun.getConfig();
					synchronized (config) {
						if (false == config.isResultSet(crun.getSU())) {
							crun.setExecutionError(result);
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
		log.debug("finish(nanosec) m_crunList.size() : {}", m_crunList.size());
		log.debug("finish(nanosec) m_crunListReady : {}", m_crunListReady);
		log.debug("finish() m_futureListReady : {}", m_futureListReady);

		try {
			m_futureCompute.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
			m_futureRetry.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
			
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCNBConnections();
			throw e;
		}

		log.debug("finish() m_futureList.size() : {}", m_futureList.size());
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_futureListReady : {}", m_futureListReady);
			gap++; // this is just for the log.debug issue mentioned above
			if (System.nanoTime() - time1 >= checkTillNanoSec)
				return false;

		}
		log.debug("finish() m_futureList.size() : {}", m_futureList.size());

        for (Future<?> f : m_futureList) {
        	String result = null;
       	
       		try {
				f.get(checkTillNanoSec, TimeUnit.NANOSECONDS);
					
			} catch (CancellationException | InterruptedException | ExecutionException e) {
				//e.printStackTrace();
				result = DMJson.error(DMUtil.clientErrMsg(e));
			
				DRangeRunner rr = getFutureRRunMap(f);
				for (int j = rr.getStartCRun(); j <= rr.getEndCRun(); j++) {
					DComputeAsyncRun crun = m_crunList.get(j);
					DComputeAsyncConfig config = crun.getConfig();
					synchronized (config) {
						if (false == config.isResultSet(crun.getSU())) {
							crun.setExecutionError(result);
						}
					}
	
				} // End for
				
			} // End catch
       
        } // End for
        closeCNBConnections();
		log.debug("finfinish(nanosec) Exiting finish()");
		initialize();
		return true;

	}

	public boolean isDone() {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("isDone() m_splitTotal : {}", m_splitTotal);
		log.debug("isDone() m_crunList.size() : {}", m_crunList.size());
		log.debug("isDone() m_crunListReady : {}", m_crunListReady);
		if (false == m_crunListReady)
			return false;
		log.debug("isDone() m_crunList.size() : {}", m_crunList.size());

		boolean check = true;

		check = m_futureCompute.isDone();
		check = m_futureRetry.isDone();
		
		log.debug("isDone() m_futureList.size() : {}", m_futureList.size());
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("isDone() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		log.debug("isDone() m_futureList.size() : {}", m_futureList.size());

		for (Future<?> f : m_futureList) {
  			check = f.isDone();
        }

		log.debug("isDone() Exiting finish()");
		return check;
	}

	
	public DComputeAsync perform() throws DScabiException {
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
			return this;
		}
		log.debug("perform() m_splitTotal : {}", m_splitTotal);
		log.debug("perform() m_maxThreads : {}", m_maxThreads);
		if (0 == m_splitTotal) {
			log.debug("m_splitTotal is zero. Cannot proceed with perform()");
			return this;
		}
		
		if (false == m_isPerformInProgress)
			m_isPerformInProgress = true;
		else {
			throw new DScabiException("Perform already in progress", "COE.PEM.1");
		}
		if (1 == m_maxThreads) {
			long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			log.debug("perform() usedMemory : {}", usedMemory);
			long freeMemory = Runtime.getRuntime().maxMemory() - usedMemory;
			log.debug("perform() freeMemory : {}", freeMemory);
			
			long noOfThreads = freeMemory / (1024 * 1024); // Assuming 1 Thread consumes 1MB Stack memory
			log.debug("perform() noOfThreads : {}", noOfThreads);
			
			if (m_splitTotal < noOfThreads) {
				m_threadPool = Executors.newFixedThreadPool(m_splitTotal + 2); // +1 to include thread for this class run() method
				//m_threadPool = new DThreadPoolExecutor(
				//		m_splitTotal + 2, m_splitTotal + 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				
				log.debug("threads created : {}", m_splitTotal + 2);
				m_noOfRangeRunners = m_splitTotal;
			} else {
				m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
				//m_threadPool = new DThreadPoolExecutor(
				//		(int)noOfThreads, (int)noOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
				log.debug("threads created : {}", noOfThreads);
				m_noOfRangeRunners = (int)noOfThreads - 2;
			}
		}
		else
		{
			m_threadPool = Executors.newFixedThreadPool(m_maxThreads);
			//m_threadPool = new DThreadPoolExecutor(
			//		m_maxThreads, m_maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
			log.debug("perform() threads created : {}", m_maxThreads);
			if (m_maxThreads > 2)
				m_noOfRangeRunners = m_maxThreads - 2;
			else
				m_noOfRangeRunners = m_maxThreads;
		}
		log.debug("m_noOfRangeRunners : {}", m_noOfRangeRunners);
        		
		m_futureCompute = m_threadPool.submit(this);
		
		return this;
	}
	
}
