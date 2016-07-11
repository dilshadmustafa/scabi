/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 24-Feb-2016
 * File Name : DComputeConfig.java
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

package com.dilmus.dilshad.scabi.core.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
//import com.dilmus.dilshad.scabi.core.async.DComputeAsyncConfig;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeSyncConfig {
	public final static int CODE = 1;
	public final static int CLASS = 2;
	public final static int OBJECT = 3;
	public final static int CLASSNAMEINJAR = 4;

	private final Logger log = LoggerFactory.getLogger(DComputeSyncConfig.class);
	private DComputeUnit m_unit = null;
	private Class<? extends DComputeUnit> m_class = null;
	private String m_code = null;
	private String m_jarFilePath = null;
	private String m_classNameInJar = null;

	private int m_configType = 0;
	private String m_jsonStrInput = null;
	private HashMap<String, String> m_outputMap = null;
	private long m_maxSplit = 1;
	private int m_maxRetry = 0;
	// Not used private HashMap<String, Boolean> m_splitStatusMap = null;
	// Not used private HashMap<String, Integer> m_failedSplitRetryMap = null;
	
	private boolean m_isSplitSet = false;
	private long m_startSplit = -1;
	private long m_endSplit = -1;

	private boolean m_isJarFilePathListSet = false;
	private LinkedList<String> m_jarFilePathList = null;

	private boolean m_isComputeUnitJarsSet = false;
	private DMClassLoader m_dcl = null;
	
	private String m_jobId = null;
	private String m_configId = null;
	
	private static final DMCounter M_DMCOUNTER = new DMCounter();
	
	public int setJobId(String jobId) {
		m_jobId = jobId;
		m_configId = jobId + "_" + M_DMCOUNTER.inc();
		return 0;
	}
	
	public String getJobId() {
		return m_jobId;
	}

	public String getConfigId() {
		return m_configId;
	}
	
	public int setComputeUnitJars(DMClassLoader dcl) {
		m_isComputeUnitJarsSet = true;
		m_dcl = dcl;
		return 0;
	}

	public boolean isComputeUnitJarsSet() {
		return m_isComputeUnitJarsSet;
	}

	public DMClassLoader getComputeUnitJars() {
		return m_dcl;
	}

	public DComputeSyncConfig(DComputeUnit unit) {
		m_unit = unit;
		m_configType = DComputeSyncConfig.OBJECT;
		m_maxSplit = 1;
		
		m_jarFilePathList = new LinkedList<String>();
		// Not used m_splitStatusMap =  new HashMap<String, Boolean>();
		// Not used m_failedSplitRetryMap = new HashMap<String, Integer>();
		m_jsonStrInput = DMJson.empty();
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}
	
	public DComputeSyncConfig(Class<? extends DComputeUnit> cls) {
		m_class = cls;
		m_configType = DComputeSyncConfig.CLASS;
		m_maxSplit = 1;
		
		m_jarFilePathList = new LinkedList<String>();
		m_jsonStrInput = DMJson.empty();
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}

	public DComputeSyncConfig(String code) {
		m_code = code;
		m_configType = DComputeSyncConfig.CODE;
		m_maxSplit = 1;
		
		m_jarFilePathList = new LinkedList<String>();
		m_jsonStrInput = DMJson.empty();
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}

	public DComputeSyncConfig(String jarFilePath, String classNameInJar) {
		m_jarFilePath = jarFilePath;
		m_classNameInJar = classNameInJar;
		m_configType = DComputeSyncConfig.CLASSNAMEINJAR;
		m_maxSplit = 1;
		
		m_jarFilePathList = new LinkedList<String>();
		m_jsonStrInput = DMJson.empty();
		
		m_jobId = DMJson.empty();
		
		// Previous works m_configId = UUID.randomUUID().toString() + "_" + System.nanoTime() + "_" + M_DMCOUNTER.inc();
		// Previous works m_configId = m_configId.replace('-', '_');
		
		m_configId = DMJson.empty();
	}

	public boolean isJarFilePathListSet() {
		return m_isJarFilePathListSet;
	}
	
	public int setJarFilePathFromList(LinkedList<String> jarFilePathList) {
		m_jarFilePathList.addAll(jarFilePathList);
		m_isJarFilePathListSet = true;
		return 0;
	}
	
	public List<String> getJarFilePathList() {
		return m_jarFilePathList;
	}

	int setSplitRange(long startSplit, long endSplit) throws DScabiException {
		
		if (startSplit <= 0) {
			throw new DScabiException("startSplit should not be <= 0", "CCG.SSR.1");
		}
		if (endSplit <= 0) {
			throw new DScabiException("endSplit should not be <= 0", "CCG.SSR.2");
		}
		if (startSplit > endSplit) {
			throw new DScabiException("startSplit should not be > endSplit", "CCG.SSR.3");
		}
		m_isSplitSet = true;
		m_startSplit = startSplit;
		m_endSplit = endSplit;

		return 0;
	}
	
	public boolean isSplitSet() {
		return m_isSplitSet;
	}
	
	public long getStartSplit() {
		return m_startSplit;
	}
	
	public long getEndSplit() {
		return m_endSplit;
	}

	/* Not used
	public int setSplitStatus(int splitno, boolean status) {
		m_splitStatusMap.put("" + splitno, status);
		return 0;
	}
	
	public boolean getSplitStatus(int splitno) {
		return m_splitStatusMap.get("" + splitno);
	}
	
	public int setOrIncFailedSplitRetryMap(int splitno) {
		if (m_failedSplitRetryMap.containsKey("" + splitno)) {
			Integer value = m_failedSplitRetryMap.get(""+ splitno);
			int retriesTillNow = value.intValue() + 1;
			Integer newRetry = new Integer(retriesTillNow);
			m_failedSplitRetryMap.put("" + splitno, newRetry);
		} else {
			Integer newRetry = new Integer(0);
			m_failedSplitRetryMap.put("" + splitno, newRetry);
		}
			
		return 0;
	}
	
	public int incIfExistsFailedSplitRetryMap(int splitno) {
		if (m_failedSplitRetryMap.containsKey("" + splitno)) {
			Integer value = m_failedSplitRetryMap.get(""+ splitno);
			int retriesTillNow = value.intValue() + 1;
			Integer newRetry = new Integer(retriesTillNow);
			m_failedSplitRetryMap.put("" + splitno, newRetry);
		} else {
			return -1; // What to do by throwing exception here?
		}
			
		return 0;
	}

	
	public HashMap<String, Integer> getFailedSplitRetryMap() {
		return m_failedSplitRetryMap;
	}
	*/
	
	public int getConfigType() {
		return m_configType;
	}
	
	public DComputeUnit getComputeUnit() {
		return m_unit;
	}

	public Class<? extends DComputeUnit> getComputeClass() {
		return m_class;
	}
	
	public String getComputeCode() {
		return m_code;
	}

	public String getJarFilePath() {
		return m_jarFilePath;
	}
	
	public String getClassNameInJar() {
		return m_classNameInJar;
	}

	public int setInput(String jsonInput) {
		m_jsonStrInput = jsonInput;
		return 0;
	}
	
	public int setOutput(HashMap<String, String> outputMap) {
		m_outputMap = outputMap;
		return 0;
	}
	
	public int setResult(long splitno, String result) {
		if (m_outputMap != null) {
			m_outputMap.put("" + splitno, result);
			log.debug("setResult() Setting result for splitno : {}, {}", splitno, result);
		}
		
		/* Debugging
    	Set<String> st = m_outputMap.keySet();
    	for (String s : st) {
    		log.debug("setResult() for {} : {}", s, m_outputMap.get(s));
    	}
		*/
		
		return 0;
	}

	public int setMaxSplit(long maxSplit) {
		m_maxSplit = maxSplit;
		return 0;
	}
	
	public int setMaxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return 0;
	}
	
	public String getInput() {
		return m_jsonStrInput;
	}
	
	public HashMap<String, String> getOutput() {
		return m_outputMap;
	}
	
	public long getMaxSplit() {
		return m_maxSplit;
	}
	
	public int getMaxRetry() {
		return m_maxRetry;
	}

}
