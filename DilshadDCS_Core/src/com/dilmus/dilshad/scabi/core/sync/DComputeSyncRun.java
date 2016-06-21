/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 24-Feb-2016
 * File Name : DComputeRun.java
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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;

import org.apache.http.NoHttpResponseException;
import org.apache.http.ParseException;
import org.apache.http.ProtocolException;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeSyncRun implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DComputeSyncRun.class);
	private DComputeSyncConfig m_config = null;
	private DComputeBlock m_computeBlock = null;
	private boolean m_isDone = false;
	private boolean m_isError = false;
	private boolean m_isRetrySubmitted = false;
	private boolean m_isRunOnce = false;
	
	private boolean m_isExecutionError = false;
	
	private long m_TU = 0;
	private long m_SU = 0;
	private int m_retriesTillNow = 0;
	private int m_maxRetry = 0;
	
	public DComputeSyncRun() {
		m_config = null;
		m_computeBlock = null;

		m_isDone = false;
		m_isError = false;
		m_isRetrySubmitted = false;
		m_isRunOnce = false;
		
		m_isExecutionError = false;
	}
	
	public int setTU(long totalUnits) {
		m_TU = totalUnits;
		return 0;
	}

	public int setSU(long splitno) {
		m_SU = splitno;
		return 0;
	}
	
	public int setMaxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return 0;
	}

	public long getTU() {
		return m_TU;
	}

	public long getSU() {
		return m_SU;
	}
	
	public int getMaxRetry() {
		return m_maxRetry;
	}
	
	public int getRetriesTillNow() {
		return m_retriesTillNow;
	}

	public boolean isError() {
		return m_isError;
	}
	
	public boolean isDone() {
		return m_isDone;
	}

	public boolean isRunOnce() {
		return m_isRunOnce;
	}
	
	public int setComputeBlock(DComputeBlock computeBlock) {
		m_computeBlock = computeBlock;
		return 0;
	}

	public int setConfig(DComputeSyncConfig config) {
		m_config = config;
		return 0;
	}
	
	public int setRetrySubmitStatus(boolean status) {
		m_isRetrySubmitted = status;
		return 0;
	}
	
	public boolean isRetrySubmitted() {
		return m_isRetrySubmitted;
	}
	
	public int setExecutionError(String errorMessage) {
		m_isExecutionError = true;
		synchronized (m_config) {
			m_config.setResult(m_SU, errorMessage);
		}
		setFlagsAfterExecutionError();
		return 0;
	}
	
	public boolean isExecutionError() {
		return m_isExecutionError;
	}
	
	private int setFlagsAfterExecutionError() {
		m_isDone = true;
		if (true == m_isError)
			m_retriesTillNow = m_retriesTillNow + 1;
		m_isError = false;
		m_isRetrySubmitted = false;
		if (false == m_isRunOnce) {
			m_isRunOnce = true;
		}
		return 0;
	}
	
	public void doRun() throws IOException, DScabiException {

		log.debug("doRun() m_SU : {}", m_SU);
		
		if (null == m_config) {
			log.debug("doRun() m_config is not set");
			throw new DScabiException("doRun() config is not set", "CRN.DRN.1");
			//return;
		}
		if (null == m_computeBlock) {
			log.debug("doRun() m_computeBlock is not set");
			throw new DScabiException("computeBlock is not set", "CRN.DRN.2");
			//return;
		}
		
		// Previous works int splitno = m_SU;
		String result = null;
		
		if (m_computeBlock.isFaulty()) {
			log.debug("doRun() m_computeBlock is marked as faulty. crun m_SU : {}", m_SU);
			String errorJsonStr = DMJson.error("m_computeBlock is marked as faulty. crun m_SU : " + m_SU);
			synchronized (m_config) {
				m_config.setResult(m_SU, errorJsonStr);
			}
			m_isError = true; 
			return;
		}

		try {
			if (m_config.isJarFilePathListSet()) {
				log.debug("doRun() isJarFilePathSet() is true");
				m_computeBlock.setJarFilePathFromList(m_config.getJarFilePathList());
			}
			m_computeBlock.setInput(m_config.getInput());
			m_computeBlock.setJobId(m_config.getJobId());
			m_computeBlock.setTaskId(m_config.getTaskId());
			if (m_config.isComputeUnitJarsSet()) {
				log.debug("doRun() isComputeUnitJarsSet() is true");
				m_computeBlock.setComputeUnitJars(m_config.getComputeUnitJars());
			}
			if (DComputeSyncConfig.OBJECT == m_config.getConfigType()) {
				log.debug("doRun() Executing for Object");
				result = m_computeBlock.executeObject(m_config.getComputeUnit());
				log.debug("doRun() result is : {}", result);
			} else if (DComputeSyncConfig.CLASS == m_config.getConfigType()){
				log.debug("doRun() Executing for Class");
				result = m_computeBlock.executeClass(m_config.getComputeClass());
				log.debug("doRun() result is : {}", result);
				
			} else if (DComputeSyncConfig.CODE == m_config.getConfigType()){
				log.debug("doRun() Executing for Code");
				result = m_computeBlock.executeCode(m_config.getComputeCode());
				log.debug("doRun() result is : {}", result);
			} else if (DComputeSyncConfig.CLASSNAMEINJAR == m_config.getConfigType()){
				log.debug("doRun() Executing for Class Name In Jar");
				result = m_computeBlock.executeClassNameInJar(m_config.getJarFilePath(), m_config.getClassNameInJar());
			} else {
				throw new DScabiException("Unknown ComputeConfig type m_config.getConfigType() " + m_config.getConfigType(), "CRN.DRN.3");
			}
			
			synchronized (m_config) {
				log.debug("doRun() Setting result for splitno m_SU : {}", m_SU);
				m_config.setResult(m_SU, result);
				// Not used m_config.setSplitStatus(splitno, true);
				// Not used m_config.incIfExistsFailedSplitRetryMap(splitno);
			}
				
		} catch (ClientProtocolException | SocketException | NoHttpResponseException e) {
			log.debug("doRun() Exception : {}", e.toString());
			// m_computeBlock is faulty only in the case of ClientProtocolException/NetworkException
			m_computeBlock.setFaulty(true);
			String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));	
			synchronized (m_config) {
				// Not used m_config.setOrIncFailedSplitRetryMap(splitno);
				// Not used m_config.setSplitStatus(splitno, false);
				m_config.setResult(m_SU, errorJsonStr);
			}
			// m_isError is set only in the case of network exception
			// only in this case retry has to be attempted if maxRetry > 0 is set by User
			m_isError = true; 
		}
		
	}

	public void run() {

		log.debug("run() m_SU : {}", m_SU);
		
		m_isDone = false;
		if (true == m_isError)
			m_retriesTillNow = m_retriesTillNow + 1;
		m_isError = false;
		
		synchronized (m_computeBlock) {
			try {
				m_computeBlock.setTU(m_TU);
				m_computeBlock.setSU(m_SU);

		        doRun();
		    } catch (Throwable e) {
				log.debug("run() Throwable : {}", e.toString());
				// m_computeBlock is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled in doRun() method
				// // // m_computeBlock.setFaulty(true);
				
				// Previous works int splitno = m_SU; // m_computeBlock.getSU(); // just to be exact, getting SU directly from m_computeBlock
				// Previous works log.debug("run() splitno : {}", splitno);

				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					// Not used m_config.setOrIncFailedSplitRetryMap(splitno);
					// Not used m_config.setSplitStatus(splitno, false);
					m_config.setResult(m_SU, errorJsonStr);
				}
	        	
		    }
			
		}
		m_isDone = true;
		m_isRetrySubmitted = false;
		if (false == m_isRunOnce) {
			m_isRunOnce = true;
		}
	}
	
}
