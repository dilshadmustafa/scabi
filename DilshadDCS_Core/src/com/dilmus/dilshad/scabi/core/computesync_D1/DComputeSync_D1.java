/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 07-Feb-2016
 * File Name : DComputeSync_D1.java
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

package com.dilmus.dilshad.scabi.core.computesync_D1;

import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

//import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.compute.DComputeAsyncConfig;
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeSync_D1 implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DComputeSync_D1.class);
	private ExecutorService m_threadPool = null;
	private DMeta m_meta = null;
	private int m_commandID = 1;
	private HashMap<String, DComputeSyncConfig_D1> m_commandMap = null;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private long m_maxSplit = 1;
	private int m_maxRetry = 3; //0;
	private long m_maxThreads = 0; //1;
	private long m_splitTotal = 0;
	private DComputeSyncConfig_D1 m_config = null;
	private boolean m_isPerformInProgress = false;
	private boolean m_crunListReady = false;
	// Not used private boolean m_futureListReady = false;

	private LinkedList<DComputeSyncConfig_D1> m_cconfigList = null;
	private LinkedList<DComputeSyncRun_D1> m_crunList = null;
	private LinkedList<DComputeBlock_D1> m_cbList = null;
	
	// Not used private LinkedList<Future<?>> m_futureList = null;
	private HashMap<Future<?>, DComputeSyncRun_D1> m_futureCRunMap = null;
	
	private Future<?> m_futureCompute = null;
	private Future<?> m_futureRetry = null;
	
	private DRetrySyncMonitor_D1 m_retryMonitor = null;
	
	private boolean m_isSplitSet = false;
	private long m_startSplit = -1;
	private long m_endSplit = -1;

	private boolean m_isJarFilePathListSet = false;
	private LinkedList<String> m_jarFilePathList = null;
	
	private String m_emptyJsonStr = Dson.empty();

	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;

	private long m_crunListSize = 0;
	private long m_cbListSize = 0;
	private long m_cbBalancedListSize = 0;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	public LinkedList<DComputeSyncConfig_D1> getCConfigList() {
		return m_cconfigList;
	}
	
	public LinkedList<DComputeSyncRun_D1> getCRunList() {
		return m_crunList;
	}
	
	public LinkedList<DComputeBlock_D1> getCBList() {
		return m_cbList;
	}
	
	public ExecutorService getExecutorService() {
		return m_threadPool;
	}
	
	public int putFutureCRunMap(Future<?> f, DComputeSyncRun_D1 crun) {
		synchronized (this) {
			m_futureCRunMap.put(f, crun);
		}
		return 0;
	}
	
	public DComputeSyncRun_D1 getFutureCRunMap(Future<?> f) {
		synchronized (this) {
			return m_futureCRunMap.get(f);
		}
	}

	public HashMap<Future<?>, DComputeSyncRun_D1> getFutureCRunMap() {
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

	public DComputeSync_D1(DMeta meta) {
		
		m_meta = meta;
		m_commandMap = new HashMap<String, DComputeSyncConfig_D1>();
		
		m_maxSplit = 1;
		m_maxRetry = 3; //0;
		m_maxThreads = 0; //1;
		m_splitTotal = 0;
		
		m_cconfigList = new LinkedList<DComputeSyncConfig_D1>();
		m_crunList = new LinkedList<DComputeSyncRun_D1>();
		m_cbList = new LinkedList<DComputeBlock_D1>();
		
		// Not used m_futureList = new LinkedList<Future<?>>();
		m_futureCRunMap = new HashMap<Future<?>, DComputeSyncRun_D1>();
		
		m_jarFilePathList = new LinkedList<String>();
		
		m_jsonStrInput = m_emptyJsonStr;
		
		m_crunListSize = 0;
		m_cbListSize = 0;
		m_cbBalancedListSize = 0;
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
		m_maxThreads = 0; //1;
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
			m_cbList.clear();
					
			// Not used m_futureList.clear();
			m_futureCRunMap.clear();
			
			m_isJarFilePathListSet = false;
			m_jarFilePathList.clear();
			
			m_isComputeUnitJarsSet = false;
			m_dcl = null;
	
			m_crunListSize = 0;
			m_cbListSize = 0;
			m_cbBalancedListSize = 0;
		}
		
		return 0;
	}
	
	public int close() {
		if (m_threadPool != null)
			m_threadPool.shutdownNow();
		m_threadPool = null;
		closeCBConnections();
		return 0;
	}
	
	public long getCRunListSize() {
		return m_crunListSize;
	}
	
	public long getCBListSize() {
		return m_cbListSize;
	}

	public int setCBListSize(long cbListSize) {
		m_cbListSize = cbListSize;
		return 0;
	}

	public DComputeSync_D1 executeCode(String code) throws DScabiException, IOException {
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
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeSyncConfig_D1(code);
		
		return this;
	}

	public DComputeSync_D1 executeClass(Class<? extends DComputeUnit> cls) throws DScabiException, IOException {
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
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeSyncConfig_D1(cls);
		
		return this;
	}

	
	public DComputeSync_D1 executeObject(DComputeUnit unit) throws DScabiException, IOException {
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
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeSyncConfig_D1(unit);
		
		return this;
	}

	public DComputeSync_D1 executeJar(String jarFilePath, String classNameInJar) throws DScabiException, IOException {
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
			m_maxRetry = 3; //0;

			m_cconfigList.add(m_config);
			
			m_config = null;
			
			m_jsonStrInput = m_emptyJsonStr;
			m_outputMap = null;
			m_isSplitSet = false;
			m_startSplit = -1;
			m_endSplit = -1;

		}
		m_config = new DComputeSyncConfig_D1(jarFilePath, classNameInJar);
		
		return this;
	}

	public DComputeSync_D1 input(String jsonStrInput) {
		m_jsonStrInput = jsonStrInput;
		return this;
	}
	
	public DComputeSync_D1 input(Dson jsonInput) {
		m_jsonStrInput = jsonInput.toString();
		return this;
	}

	public DComputeSync_D1 input(Properties propertyInput) {
		Dson dson = new Dson();
		Set<String> st = propertyInput.stringPropertyNames();
		for (String s : st) {
			dson = dson.add(s, propertyInput.getProperty(s));
		}
		m_jsonStrInput = dson.toString();
		return this;
	}

	public DComputeSync_D1 input(HashMap<String, String> mapInput) {
		Dson dson = new Dson();
		Set<String> st = mapInput.keySet();
		for (String s : st) {
			dson = dson.add(s, mapInput.get(s));
			
		}
		m_jsonStrInput = dson.toString();
		return this;
	}
	
	public DComputeSync_D1 output(HashMap<String, String> outputMap) {
		m_outputMap = outputMap;
		return this;
	}

	public DComputeSync_D1 split(long maxSplit) throws DScabiException {
		if (maxSplit <= 0)
			throw new DScabiException("Split should not be <= 0" , "COE.SPT.1");
		m_maxSplit = maxSplit;
		return this;
	}
	
	public DComputeSync_D1 splitRange(long startSplit, long endSplit) throws DScabiException {
		
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

	public DComputeSync_D1 maxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return this;
	}
	
	public DComputeSync_D1 maxThreads(long maxThreads) {
		m_maxThreads = maxThreads;
		return this;
	}
	
	/* Not used
	public int addToFutureList(Future<?> f) {
		synchronized (this) {
			m_futureList.add(f);
		}
		return 0;
	}
	
	public Future<?> getFromFutureList(long index) {
		synchronized (this) {
			// Previous works return m_futureList.get(index);
			ListIterator<Future<?>> itr = DMUtil.iteratorBefore(m_futureList, index);
			return itr.next();

		}
	}
	*/
	
	private LinkedList<DComputeBlock_D1> balance(LinkedList<DComputeBlock_D1> cba) {
		
		boolean check = true;
		long current = m_cbListSize; // Previous works cba.size();
		LinkedList<DComputeBlock_D1> cbaBalanced = new LinkedList<DComputeBlock_D1>();
		DComputeBlock_D1 another = null;
		
		cbaBalanced.addAll(cba);
		m_cbBalancedListSize = m_cbListSize;
		while (check && current < m_splitTotal) {
			
			for (DComputeBlock_D1 cb : cba) {
				try {
					another = cb.another();
				} catch (Error | RuntimeException e) {
					log.debug("balance() Client Side Error/Exception occurred");
					log.debug("balance() returning cbs created till now");
					e.printStackTrace();
					return cbaBalanced;
				} catch (Exception e) {
					log.debug("balance() Client Side Error/Exception occurred");
					log.debug("balance() returning cbs created till now");
					e.printStackTrace();
					return cbaBalanced;
				} catch (Throwable e) {
					log.debug("balance() Client Side Error/Exception occurred");
					log.debug("balance() returning cbs created till now");
					e.printStackTrace();
					return cbaBalanced;
				}
				if (null == another)
					continue;
				cbaBalanced.add(another);
				current++;
				m_cbBalancedListSize++;
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
		return cbaBalanced;
	}
	
	private int getComputeBlockMany(long splitTotal) throws /*ParseException,*/ IOException, DScabiException, DScabiClientException {
		
		String jsonString = m_meta.getComputeManyJsonStr(splitTotal);
		
		DMJson djson = new DMJson(jsonString);
		long count = djson.getCount();
		Set<String> st = djson.keySet();
		if (0 == count)
			throw new DScabiException("Zero Compute Server available", "CAC.GCN.1");
		for (String s : st) {
			if (s.equals("Count"))
				continue;
			m_cbList.add(new DComputeBlock_D1(djson.getString(s)));
			m_cbListSize++;
		}

		return 0;
	}

	public void run() {
		
		// Previous works List<DComputeBlock> cba = null;
		// Previous works List<DComputeBlock> cbaoriginal = null;
		
		try {
			/* Previous works
			cbaoriginal = m_meta.getComputeMany(m_splitTotal);
			if (null == cbaoriginal)
				throw new DScabiClientException("Zero Compute Server available", "COE.RUN.1");
			cba = balance(cbaoriginal);
			log.debug("run() cbaoriginal.size() : {}", cbaoriginal.size());
			log.debug("run() cba.size() : {}", cba.size());
			*/
			if (m_splitTotal > 1)
				getComputeBlockMany(m_splitTotal);
			else
				getComputeBlockMany(m_splitTotal + 1);
			log.debug("run() m_cbListSize : {}", m_cbListSize);
			m_cbList = balance(m_cbList);
			m_cbListSize = m_cbBalancedListSize;
			log.debug("run() after balance, m_cbListSize : {}", m_cbListSize);
			
		} catch (/*ParseException |*/ IOException | DScabiException | DScabiClientException e) {
			//e.printStackTrace();
			throw new RuntimeException(e);
		} catch (Error | RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} catch (Throwable e) {
			throw e;
		}
		
		/* Previous works
        for (DComputeBlock cb : cba) {
        	log.debug("Compute is {}", cb);
        	m_cbList.add(cb);
        }
        */
		
		ListIterator<DComputeBlock_D1> itr = m_cbList.listIterator();
        Set<String> st = m_commandMap.keySet();
        // Previous works int k = 0;
        for (String key : st) {
        	DComputeSyncConfig_D1 config = m_commandMap.get(key);
			long maxSplit = config.getMaxSplit();
			log.debug("maxSplit for this config : {}", maxSplit);
        	long startSplit = 1;
        	long endSplit = maxSplit;
			if (config.isSplitSet()) {
				startSplit = config.getStartSplit();
				log.debug("run() startSplit for this config : {}", startSplit);
				endSplit = config.getEndSplit();
				log.debug("run() endSplit for this config : {}", endSplit);
			}

			// Moved to above ListIterator<DComputeBlock> itr = m_cbList.listIterator();
        	// Previous works for (int i = 0; i < maxSplit; i++) {
        	for (long i = startSplit; i <= endSplit; i++) {
        		log.debug("Inside split for loop");
        		// Previous works if (k >= cba.size())
    	        // Previous works	k = 0;
            	DComputeSyncRun_D1 crun = new DComputeSyncRun_D1(); 
            	m_crunList.add(crun);
            	m_crunListSize++;
            	crun.setConfig(config);
            	crun.setTU(maxSplit);
            	// Previous works crun.setSU(i + 1);
            	crun.setSU(i);
            	crun.setMaxRetry(config.getMaxRetry());
            	// Previous works crun.setComputeBlock(cba.get(k));
        		if (itr.hasNext())
                	crun.setComputeBlock(itr.next());
        		else {
        			itr = m_cbList.listIterator();
        			crun.setComputeBlock(itr.next());
        		}
     			// Previous works k++;
         	}
        }
        // Previous works log.debug("run() m_crunList.size() : {}", m_crunList.size());
        log.debug("run() m_crunListSize : {}", m_crunListSize);
        
        m_crunListReady = true;
        
        for (DComputeSyncRun_D1 crun : m_crunList) {
        	
        	Future<?> f = m_threadPool.submit(crun);
        	// Reference Future<ComputeRun> f = m_threadPool.submit(crun, crun);
        	
        	// Not used addToFutureList(f);
        	putFutureCRunMap(f, crun);
        	
        }
        // Not used m_futureListReady = true;
        
        // Lock order used by RetryMonitor thread is C, CS, CC, C
        // To prevent deadlock with this thread, start RetryMonitor thread at the end
        m_retryMonitor = new DRetrySyncMonitor_D1(this, m_meta);
        m_futureRetry = m_threadPool.submit(m_retryMonitor);
        
	}
	
	public boolean finish() throws DScabiException, ExecutionException, InterruptedException {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("finish() m_splitTotal : {}", m_splitTotal);
		// Previous works log.debug("finish() m_crunList.size() : {}", m_crunList.size());
        log.debug("finish() m_crunListSize : {}", m_crunListSize);
		log.debug("finish() m_crunListReady : {}", m_crunListReady);
		
		try {
			m_futureCompute.get();
			m_futureRetry.get();
			// Put m_futureRetry.get() here so that if DRetrySyncMonitor goes down
			// when a particular crun's m_isError is true, m_isDone is true
			// the below while(check) will not go into infinite loop
			// In case of crun Execution error, DThreadPoolExecutor's afterExecute() will reset crun's flags
			// appropriately in another thread which executed the crun
			// This place differs from how it is handled in DCompute class
			// DComputeSync class - variable futures for threads, crun flags reset in another thread which 
			// executed the crun using DThreadPoolExecutor's afterExecute() method
			// For DCompute class - fixed futures for threads, crun flags reset by finish() method in the 
			// driver's or master's thread
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCBConnections();
			throw e;
		}
		
		int gap = 0;
		while (false == m_crunListReady) {
			// TODO log.debug from this point onwards doesn't work if we don't use log.debug here!!!
			if (0 == gap % 1000000000) // reduce this number if needed to make it print atleast once
				log.debug("finish() m_crunListReady : {}", m_crunListReady);
			gap++; // this is just for the log.debug issue mentioned above
		}
		// Previous works log.debug("finish() m_crunList.size() : {}", m_crunList.size());
        log.debug("finish() m_crunListSize : {}", m_crunListSize);
        
		boolean check = true;
		while(check) {
			for (DComputeSyncRun_D1 crun : m_crunList) {
				
				synchronized(crun) {
				
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
				
				} // End synchronized crun
			}
			if (check)
				break;	
			check = true;
		}
		
		/* TODO Further analysis
		try {
			m_futureRetry.get();
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCBConnections();
			throw e;
		}
		*/
		
		closeCBConnections();
		log.debug("finish() Exiting finish()");
		initialize();
		return true;
				
	}
	
	private int closeCBConnections() {
		
		if (null == m_retryMonitor)
			return 0;
		LinkedList<DComputeBlock_D1> cba = m_retryMonitor.getCBList();
		if (null == cba)
			return 0;
		for (DComputeBlock_D1 cb : cba) {
			try {
				cb.close();
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
		// Previous works log.debug("finish(nanosec) m_crunList.size() : {}", m_crunList.size());
        log.debug("finish(nanosec) m_crunListSize : {}", m_crunListSize);
		log.debug("finish(nanosec) m_crunListReady : {}", m_crunListReady);

		try {
			m_futureCompute.get();
			m_futureRetry.get();
			// Put m_futureRetry.get() here so that if DRetrySyncMonitor goes down
			// when a particular crun's m_isError is true, m_isDone is true
			// the below while(check) will not go into infinite loop
			// In case of crun Execution error, DThreadPoolExecutor's afterExecute() will reset crun's flags
			// appropriately in another thread which executed the crun
			// This place differs from how it is handled in DCompute class
			// DComputeSync class - variable futures for threads, crun flags reset in another thread which 
			// executed the crun using DThreadPoolExecutor's afterExecute() method
			// For DCompute class - fixed futures for threads, crun flags reset by finish() method in the 
			// driver's or master's thread
			
		} catch (CancellationException | InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			closeCBConnections();
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
		// Previous works log.debug("finish(nanosec) m_crunList.size() : {}", m_crunList.size());
        log.debug("finish(nanosec) m_crunListSize : {}", m_crunListSize);
        
		boolean check = true;
		while(check) {
			for (DComputeSyncRun_D1 crun : m_crunList) {
				
				synchronized(crun) {
				
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
				
				} // End synchronized crun
			}
			if (check)
				break;	
			check = true;
			if (System.nanoTime() - time1 >= checkTillNanoSec)
				return false;

		}
		closeCBConnections();

		log.debug("finish(nanosec) Exiting finish()");
		initialize();
		return true;
	}

	public boolean isDone() {
		if (false == m_isPerformInProgress)
			return true;
		log.debug("isDone() m_splitTotal : {}", m_splitTotal);
		// Previous works log.debug("isDone() m_crunList.size() : {}", m_crunList.size());
        log.debug("isDone() m_crunListSize : {}", m_crunListSize);
		log.debug("isDone() m_crunListReady : {}", m_crunListReady);
		if (false == m_crunListReady)
			return false;
        
		boolean check = true;
		
		check = m_futureCompute.isDone();
		if (false == check)
			return false;
		check = m_futureRetry.isDone();
		if (false == check)
			return false;

		// Previous works log.debug("isDone() m_crunList.size() : {}", m_crunList.size());
        log.debug("isDone() m_crunListSize : {}", m_crunListSize);

		for (DComputeSyncRun_D1 crun : m_crunList) {
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
	
	public DComputeSync_D1 perform() throws DScabiException, IOException {
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
			m_maxRetry = 3; //0;

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
		
		String jobId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		jobId = jobId.replace('-', '_');
		for (DComputeSyncConfig_D1 config : m_cconfigList) {
			config.setJobId(jobId);
			config.combineJarsForAddJarJsonField();
		}
		
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
					m_threadPool = new DThreadPoolExecutor_D1(
							(int)(m_splitTotal + 1), (int)(m_splitTotal + 1), 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
					log.debug("threads created : {}", m_splitTotal + 1);
				}
				else if (noOfThreads < Integer.MAX_VALUE) {
					m_threadPool = new DThreadPoolExecutor_D1(
							(int)noOfThreads, (int)noOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
					log.debug("threads created : {}", noOfThreads);
				}
				else {
					m_threadPool = new DThreadPoolExecutor_D1(
							(int)Integer.MAX_VALUE, (int)Integer.MAX_VALUE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
					log.debug("threads created : {}", Integer.MAX_VALUE);
				}
			} else {
				if (noOfThreads < Integer.MAX_VALUE) {
					m_threadPool = new DThreadPoolExecutor_D1(
							(int)noOfThreads, (int)noOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
					log.debug("threads created : {}", noOfThreads);
				}	
				else {
					m_threadPool = new DThreadPoolExecutor_D1(
							(int)Integer.MAX_VALUE, (int)Integer.MAX_VALUE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
					log.debug("threads created : {}", Integer.MAX_VALUE);
				}
			}

			/* works
			if (m_splitTotal < noOfThreads) {
				// Previous works m_threadPool = Executors.newFixedThreadPool(m_splitTotal + 2); // +1 to include thread for this class run() method
				m_threadPool = new DThreadPoolExecutor(
						(int)(m_splitTotal + 2), (int)(m_splitTotal + 2), 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				
				log.debug("threads created : {}", m_splitTotal + 2);
			} else {
				// Previous works m_threadPool = Executors.newFixedThreadPool((int)noOfThreads);
				m_threadPool = new DThreadPoolExecutor(
						(int)noOfThreads, (int)noOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
				log.debug("threads created : {}", noOfThreads);
			}
			*/
		}
		else
		{
			if (m_maxThreads < Integer.MAX_VALUE) {
				m_threadPool = new DThreadPoolExecutor_D1(
						(int)m_maxThreads, (int)m_maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				log.debug("perform() threads created : {}", m_maxThreads);
			}
			else {
				m_threadPool = new DThreadPoolExecutor_D1(
						(int)Integer.MAX_VALUE, (int)Integer.MAX_VALUE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
				log.debug("perform() threads created : {}", Integer.MAX_VALUE);
			}

			/* works
			// Previous works m_threadPool = Executors.newFixedThreadPool(m_maxThreads);
			m_threadPool = new DThreadPoolExecutor(
					(int)m_maxThreads, (int)m_maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this);
			
			log.debug("perform() threads created : {}", m_maxThreads);
			*/
		}
        		
		m_futureCompute = m_threadPool.submit(this);
		
		return this;
	}
	
}
