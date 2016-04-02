/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 07-Feb-2016
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

package com.dilmus.dilshad.scabi.core;

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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.async.DComputeAsync;

/**
 * @author Dilshad Mustafa
 *
 */
public class DCompute implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DCompute.class);
	private ExecutorService m_threadPool = null;
	private DMeta m_meta = null;
	private int m_commandID = 1;
	private HashMap<String, DComputeConfig> m_commandMap = null;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private int m_maxSplit = 1;
	private int m_maxRetry = 0;
	private int m_maxThreads = 1;
	private int m_splitTotal = 0;
	private DComputeConfig m_config = null;
	private boolean m_isPerformInProgress = false;
	private boolean m_crunListReady = false;
	// Not used private boolean m_futureListReady = false;

	private List<DComputeConfig> m_cconfigList = null;
	private List<DComputeRun> m_crunList = null;
	private List<DComputeSync> m_csyncList = null;
	
	// Not used private List<Future<?>> m_futureList = null;
	private HashMap<Future<?>, DComputeRun> m_futureCRunMap = null;
	// Not use private List<ComputeSync> m_csyncWorkingList = null;
	// Not used private List<ComputeRun> m_crunForRetryList = null;
	
	private Future<?> m_futureCompute = null;
	private Future<?> m_futureRetry = null;
	
	private DRetryMonitor m_retryMonitor = null;
	
	private boolean m_isSplitSet = false;
	private int m_startSplit = -1;
	private int m_endSplit = -1;

	private boolean m_isJarFilePathListSet = false;
	private List<String> m_jarFilePathList = null;
	
	private String m_emptyJsonStr = Dson.empty();

	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;

	public List<DComputeConfig> getCConfigList() {
		return m_cconfigList;
	}
	
	public List<DComputeRun> getCRunList() {
		return m_crunList;
	}
	
	public List<DComputeSync> getCSyncList() {
		return m_csyncList;
	}
	
	public ExecutorService getExecutorService() {
		return m_threadPool;
	}
	
	public int putFutureCRunMap(Future<?> f, DComputeRun crun) {
		synchronized (this) {
			m_futureCRunMap.put(f, crun);
		}
		return 0;
	}
	
	public DComputeRun getFutureCRunMap(Future<?> f) {
		synchronized (this) {
			return m_futureCRunMap.get(f);
		}
	}

	public HashMap<Future<?>, DComputeRun> getFutureCRunMap() {
			return m_futureCRunMap;
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
		m_commandMap = new HashMap<String, DComputeConfig>();
		
		m_maxSplit = 1;
		m_maxRetry = 0;
		m_maxThreads = 1;
		m_splitTotal = 0;
		
		m_cconfigList = new ArrayList<DComputeConfig>();
		m_crunList = new ArrayList<DComputeRun>();
		m_csyncList = new ArrayList<DComputeSync>();
		
		// Not used m_futureList = new ArrayList<Future<?>>();
		m_futureCRunMap = new HashMap<Future<?>, DComputeRun>();
		// Not used m_crunForRetryList = null;
		
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
		// Not used m_futureListReady = false;

		synchronized (this) {
			
		m_cconfigList.clear();
		// m_crunList is not thread safe and RetryMonitor is also using it
		// So it is cleared after thread pool shutdown
		m_crunList.clear();
		m_csyncList.clear();
				
		// Not used m_futureList.clear();
		m_futureCRunMap.clear();
		
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
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "COE.ECE.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "COE.ECE.3");
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
		m_config = new DComputeConfig(code);
		
		return this;
	}

	public DCompute executeClass(Class<? extends DComputeUnit> cls) throws DScabiException {
		if (m_isPerformInProgress) {
			throw new DScabiException("Perform already in progress", "COE.ECS.1");
		}
		if (m_config != null) {
			m_config.setInput(m_jsonStrInput);
			m_config.setOutput(m_outputMap);
			m_config.setMaxSplit(m_maxSplit);
			m_config.setMaxRetry(m_maxRetry);
			
			if (m_isSplitSet) {
				if (m_startSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "COE.ECS.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "COE.ECS.3");
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
		m_config = new DComputeConfig(cls);
		
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
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "COE.EOT.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "COE.EOT.3");
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
		m_config = new DComputeConfig(unit);
		
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
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "COE.ECN.2");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "COE.ECN.3");
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
		m_config = new DComputeConfig(jarFilePath, classNameInJar);
		
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
		// outputTo()
		m_outputMap = outputMap;
		return this;
	}

	public DCompute split(int maxSplit) throws DScabiException {
		if (maxSplit <= 0)
			throw new DScabiException("Split should not be <= 0" , "COE.SPT.1");
		m_maxSplit = maxSplit;
		return this;
	}
	
	public DCompute splitRange(int startSplit, int endSplit) throws DScabiException {
		
		if (startSplit <= 0) {
			throw new DScabiException("startSplit should not be <= 0", "COE.SRE.1");
		}
		if (endSplit <= 0) {
			throw new DScabiException("endSplit should not be <= 0", "COE.SRE.2");
		}
		if (startSplit > endSplit) {
			throw new DScabiException("startSplit should not be > endSplit", "COE.SRE.3");
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
	
	public DCompute maxThreads(int maxThreads) {
		m_maxThreads = maxThreads;
		return this;
	}
	
	/* Not used. Previous working version without retry mechanism
	public void run() {
		//ComputeSync csync = m_meta.getCompute();
		//ComputeSync csynca[] = m_meta.getComputeMany(m_commandID);
		ComputeSync csynca[];
		try {
			csynca = m_meta.getComputeMany(2);
		} catch (ParseException | IOException | ScabiClientException e) {
			
			e.printStackTrace();
			return;
		}
		
        for (ComputeSync csync : csynca) {
        	log.debug("Compute is {}", csync);
        }

        Set<String> st = m_commandMap.keySet();
        int k = 0;
        for (String key : st) {
	        //if (k >= csynca.length)
	        //	k = 0;
        	ComputeConfig config = m_commandMap.get(key);
			int maxSplit = config.getMaxSplit();
			log.debug("maxSplit : {}", maxSplit);
        	for (int i = 0; i < maxSplit; i++) {
        		log.debug("Inside split for loop");
        		if (k >= csynca.length)
    	        	k = 0;
            	ComputeRun cr = new ComputeRun(); 
            	cr.setConfig(config);
            	synchronized(csynca[k]) {
	            	csynca[k].setTU(maxSplit);
	            	csynca[k].setSU(i + 1);
            	}
            	cr.setComputeSync(csynca[k]);
    			k++;
    			m_threadPool.execute(cr);
        	}
        }
		
	}
	*/
	/* Not used
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
	*/
	
	private List<DComputeSync> balance(List<DComputeSync> csynca) {
		
		boolean check = true;
		int current = csynca.size();
		List<DComputeSync> csyncaBalanced = new ArrayList<DComputeSync>();
		DComputeSync another = null;
		
		csyncaBalanced.addAll(csynca);
		while (check && current < m_splitTotal) {
			
			for (DComputeSync csync : csynca) {
				try {
					another = csync.another();
				} catch (Error | RuntimeException e) {
					log.debug("balance() Client Side Error/Exception occurred");
					log.debug("balance() returning csyncs created till now");
					e.printStackTrace();
					return csyncaBalanced;
				} catch (Exception e) {
					log.debug("balance() Client Side Error/Exception occurred");
					log.debug("balance() returning csyncs created till now");
					e.printStackTrace();
					return csyncaBalanced;
				} catch (Throwable e) {
					log.debug("balance() Client Side Error/Exception occurred");
					log.debug("balance() returning csyncs created till now");
					e.printStackTrace();
					return csyncaBalanced;
				}
				
				if (null == another)
					continue;
				csyncaBalanced.add(another);
				current++;
				if (current >= m_splitTotal) {
					check = true;
					break;
				} else 
					check = false;
			}
			if (check)
				break;
			check = true;
		}
		return csyncaBalanced;
	}
	
	public void run() {
		
		//works DComputeSync csynca[];
		List<DComputeSync> csynca = null;
		List<DComputeSync> csyncaoriginal = null;
		
		try {
			csyncaoriginal = m_meta.getComputeMany(m_splitTotal);
			if (null == csyncaoriginal)
				throw new DScabiClientException("Zero Compute Server available", "COE.RUN.1");
			csynca = balance(csyncaoriginal);
			log.debug("run() csyncaoriginal.size() : {}", csyncaoriginal.size());
			log.debug("run() csynca.size() : {}", csynca.size());
			//csynca = m_meta.getComputeMany(1);
		} catch (ParseException | IOException | DScabiClientException e) {
			//e.printStackTrace();
			throw new RuntimeException(e);
		} catch (Error | RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} catch (Throwable e) {
			throw e;
		}
		
        for (DComputeSync csync : csynca) {
        	log.debug("Compute is {}", csync);
        	m_csyncList.add(csync);
        }
        
        Set<String> st = m_commandMap.keySet();
        int k = 0;
        for (String key : st) {
        	DComputeConfig config = m_commandMap.get(key);
			int maxSplit = config.getMaxSplit();
			log.debug("maxSplit for this config : {}", maxSplit);
        	int startSplit = 1;
        	int endSplit = maxSplit;
			if (config.isSplitSet()) {
				startSplit = config.getStartSplit();
				log.debug("run() startSplit for this config : {}", startSplit);
				endSplit = config.getEndSplit();
				log.debug("run() endSplit for this config : {}", endSplit);
			}

        	// works for (int i = 0; i < maxSplit; i++) {
        	for (int i = startSplit; i <= endSplit; i++) {
        		log.debug("Inside split for loop");
        		//works if (k >= csynca.length)
    	        //works 	k = 0;
        		if (k >= csynca.size())
    	        	k = 0;
            	DComputeRun crun = new DComputeRun(); 
            	m_crunList.add(crun);
            	crun.setConfig(config);
            	crun.setTU(maxSplit);
            	// works crun.setSU(i + 1);
            	crun.setSU(i);
            	crun.setMaxRetry(config.getMaxRetry());
            	//works crun.setComputeSync(csynca[k]);
            	crun.setComputeSync(csynca.get(k));
     			k++;
         	}
        }
        log.debug("run() m_crunList.size() : {}", m_crunList.size()); 
        m_crunListReady = true;
        
        for (DComputeRun crun : m_crunList) {
        	
        	Future<?> f = m_threadPool.submit(crun);
        	// Reference Future<ComputeRun> f = m_threadPool.submit(crun, crun);
        	
        	// Not used addToFutureList(f);
        	putFutureCRunMap(f, crun);
        	
        }
        // Not used m_futureListReady = true;
        
        // Lock order used by RetryMonitor thread is C, CS, CC, C
        // To prevent deadlock with this thread, start RetryMonitor thread at the end
        m_retryMonitor = new DRetryMonitor(this, m_meta);
        m_futureRetry = m_threadPool.submit(m_retryMonitor);
        // Not used m_futureList.add(m_futureRetryMonitor);
        
	}

	/* Not used. Previous attempt at retry mechanism within same thread
	public int handleRetry() throws ParseException, IOException, ScabiClientException {
		int fcTotal = 0;
		int csyncWorkingTotal = 0;
		m_csyncWorkingList = new ArrayList<ComputeSync>();
		m_csyncWorkingList.addAll(m_csyncList);
		
		for (ComputeConfig cconfig : m_cconfigList) {
			HashMap<String, Integer> mapRetry = cconfig.getFailedSplitRetryMap();
			Set<String> st = mapRetry.keySet();
			for (String splitno : st) {
				Integer retriesTillNow = mapRetry.get(splitno);
				if (retriesTillNow < cconfig.getMaxRetry() && false == cconfig.getSplitStatus(Integer.parseInt(splitno)))
					fcTotal = fcTotal + 1;
			}
		}
		if (0 == fcTotal) {
			log.debug("fcTotal : {}", fcTotal);
			return 0;
		}
		for (ComputeSync csync : m_csyncWorkingList) {
			if (csync.isFaulty()) {
				m_csyncWorkingList.remove(csync);
			}
		}
		csyncWorkingTotal = m_csyncWorkingList.size();
		
		if (fcTotal > csyncWorkingTotal) {
			ComputeSync csynca[] = m_meta.getComputeMany(fcTotal - csyncWorkingTotal);
			for (ComputeSync csync : csynca) {
				m_csyncWorkingList.add(csync);
			}
		}
		int k = 0;
		for (ComputeConfig cconfig : m_cconfigList) {
			HashMap<String, Integer> mapRetry = cconfig.getFailedSplitRetryMap();
			Set<String> st = mapRetry.keySet();
			for (String splitno : st) {
				Integer retriesTillNow = mapRetry.get(splitno);
				if (retriesTillNow < cconfig.getMaxRetry() && false == cconfig.getSplitStatus(Integer.parseInt(splitno))) {
	        		log.debug("Inside split for loop");
	        		if (k >= m_csyncWorkingList.size())
	    	        	k = 0;
	            	ComputeRun crun = new ComputeRun(); 
	            	m_crunForRetryList.add(crun);
	            	crun.setConfig(cconfig);
	            	ComputeSync csync = m_csyncWorkingList.get(k);
	            	int maxSplit = cconfig.getMaxSplit();
	            	synchronized(csync) {
		            	csync.setTU(maxSplit);
		            	csync.setSU(Integer.parseInt(splitno));
	            	}
	            	crun.setComputeSync(csync);
	    			k++;
	    			m_threadPool.execute(crun);
				}
			}
		}
		
		boolean check = true;
		while(check) {
			for (ComputeRun crun : m_crunForRetryList) {
				if (false == crun.isDone()) {
					check = false;
					break;
				}
			}
			if (check)
				break;	
			check = true;
		}
		return 0;
	}
	*/
	
	public boolean finish() throws DScabiException, ExecutionException, InterruptedException {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("finish() m_splitTotal : {}", m_splitTotal);
		log.debug("finish() m_crunList.size() : {}", m_crunList.size());
		log.debug("finish() m_crunListReady : {}", m_crunListReady);
		
		try {
			m_futureCompute.get();
			m_futureRetry.get();
			
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCSyncConnections();
			throw e;
		}
		
		int gap = 0;
		while (false == m_crunListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_crunListReady : {}", m_crunListReady);
			gap++; // this is just for the log.debug issue mentioned above
		}
		log.debug("finish() m_crunList.size() : {}", m_crunList.size());

		boolean check = true;
		while(check) {
			for (DComputeRun crun : m_crunList) {
				if (crun.getRetriesTillNow() < crun.getMaxRetry() && true == crun.isError() && true == crun.isDone() ) {
					check = false;
					break;
				} else if (false == crun.isDone()) {
					check = false;
					break;
				} else if (true == crun.isRetrySubmitted()) {
					check = false;
					break;
				} else if (true == crun.isExecutionError()) {
					; // don't set anything. Consider the crun to be done. Proceed with next crun check
				} 
				
			}
			if (check)
				break;	
			check = true;
		}
		closeCSyncConnections();

		log.debug("finish() Exiting finish()");
		initialize();
		return true;
		
		/*
		log.debug("finish() m_futureList.size() : {}", m_futureList.size());
		log.debug("finish() m_futureListReady : {}", m_futureListReady);
		int gap = 0;
		while (false == m_futureListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_futureListReady : {}", m_futureListReady);
			gap++;
		}
		log.debug("finish() m_futureList.size() : {}", m_futureList.size());

		Throwable t = null;
		for (Future<?> f : m_futureList) {
			log.debug("finish() Inside get() loop");
			try {
				if (f.get() != null)
					throw new DScabiException("f.get() != null", "COE.FIH.1");
			} catch (CancellationException ce) {
	            //t = ce;
	            throw ce;
	        } catch (ExecutionException ee) {
	            //t = ee.getCause();
	            throw ee;
	        } catch (InterruptedException ie) {
	            //Thread.currentThread().interrupt(); // ignore/reset
	            throw ie;
	        }
		}
		log.debug("finish() Exiting finish()");
		initialize();
		return true;
		*/
		
	}
	
	private int closeCSyncConnections() {
		
		if (null == m_retryMonitor)
			return 0;
		List<DComputeSync> csynca = m_retryMonitor.getCSyncList();
		if (null == csynca)
			return 0;
		for (DComputeSync csync : csynca) {
			try {
				csync.close();
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

		try {
			m_futureCompute.get();
			m_futureRetry.get();
			
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCSyncConnections();
			throw e;
		}
		
		int gap = 0;
		while (false == m_crunListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish(nanosec) m_crunListReady : {}", m_crunListReady);
			gap++; // this is just for the log.debug issue mentioned above
			if (System.nanoTime() - time1 >= checkTillNanoSec)
				return false;
		}
		log.debug("finish(nanosec) m_crunList.size() : {}", m_crunList.size());

		boolean check = true;
		while(check) {
			for (DComputeRun crun : m_crunList) {
				if (crun.getRetriesTillNow() < crun.getMaxRetry() && true == crun.isError() && true == crun.isDone() ) {
					check = false;
					break;
				} else if (false == crun.isDone()) {
					check = false;
					break;
				} else if (true == crun.isRetrySubmitted()) {
					check = false;
					break;
				} else if (true == crun.isExecutionError()) {
					; // don't set anything. Consider the crun to be done. Proceed with next crun check
				}
				
			}
			if (check)
				break;	
			check = true;
			if (System.nanoTime() - time1 >= checkTillNanoSec)
				return false;

		}
		closeCSyncConnections();

		log.debug("finish(nanosec) Exiting finish()");
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
			for (DComputeRun crun : m_crunList) {
				if (crun.getRetriesTillNow() < crun.getMaxRetry() && true == crun.isError() && true == crun.isDone() ) {
					check = false;
					break;
				} else if (false == crun.isDone()) {
					check = false;
					break;
				} else if (true == crun.isRetrySubmitted()) {
					check = false;
					break;
				}
				
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
					throw new DScabiException("For previous execute(), startSplit should not be > maxSplit", "COE.PEM.1");
				else if (m_endSplit > m_maxSplit)
					throw new DScabiException("For previous execute(), endSplit should not be > maxSplit", "COE.PEM.2");
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
		if (false == m_isPerformInProgress)
			m_isPerformInProgress = true;
		else {
			throw new DScabiException("Perform already in progress", "COE.PEM.1");
		}
		log.debug("perform() m_splitTotal : {}", m_splitTotal);
		log.debug("perform() m_maxThreads : {}", m_maxThreads);
		if (1 == m_maxThreads) {
			long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			log.debug("perform() usedMemory : {}", usedMemory);
			long freeMemory = Runtime.getRuntime().maxMemory() - usedMemory;
			log.debug("perform() freeMemory : {}", freeMemory);
			
			long noOfThreads = freeMemory / (1024 * 1024); // Assuming 1 Thread consumes 1MB Stack memory
			log.debug("perform() noOfThreads : {}", noOfThreads);
			
			if (m_splitTotal < noOfThreads) {
				// works m_threadPool = Executors.newFixedThreadPool(m_splitTotal + 2); // +1 to include thread for this class run() method
				m_threadPool = new DThreadPoolExecutor(
						m_splitTotal + 2, m_splitTotal + 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				
				log.debug("threads created : {}", m_splitTotal + 2);
			} else {
				// works m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
				m_threadPool = new DThreadPoolExecutor(
						(int)noOfThreads, (int)noOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
				log.debug("threads created : {}", noOfThreads);
			}

		}
		else
		{
			// works m_threadPool = Executors.newFixedThreadPool(m_maxThreads);
			m_threadPool = new DThreadPoolExecutor(
					m_maxThreads, m_maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
			log.debug("perform() threads created : {}", m_maxThreads);
		}
        		
		m_futureCompute = m_threadPool.submit(this);
		// Not used m_futureList.add(f);
		return this;
	}
	
}
