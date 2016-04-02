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
import com.dilmus.dilshad.scabi.core.async.DComputeAsyncConfig;

/**
 * @author Dilshad Mustafa
 *
 */
public class DComputeRun implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DComputeRun.class);
	private DComputeConfig m_config = null;
	private DComputeSync m_computeSync = null;
	private boolean m_isDone = false;
	private boolean m_isError = false;
	private boolean m_isRetrySubmitted = false;
	private boolean m_isRunOnce = false;
	
	private boolean m_isExecutionError = false;
	
	private int m_TU = 0;
	private int m_SU = 0;
	private int m_retriesTillNow = 0;
	private int m_maxRetry = 0;
	
	public DComputeRun() {
		m_config = null;
		m_computeSync = null;

		m_isDone = false;
		m_isError = false;
		m_isRetrySubmitted = false;
		m_isRunOnce = false;
		
		m_isExecutionError = false;
	}
	
	public int setTU(int totalUnits) {
		m_TU = totalUnits;
		return 0;
	}

	public int setSU(int splitno) {
		m_SU = splitno;
		return 0;
	}
	
	public int setMaxRetry(int maxRetry) {
		m_maxRetry = maxRetry;
		return 0;
	}

	public int getTU() {
		return m_TU;
	}

	public int getSU() {
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
	
	public int setComputeSync(DComputeSync computeSync) {
		m_computeSync = computeSync;
		return 0;
	}

	public int setConfig(DComputeConfig config) {
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
	
	/* Not used. Previous working version without retry mechanism
	public void run() {
		try {
			Thread.sleep(10000);	
		} catch (Exception e) { 
			
		}
		if (null == m_config) {
			//throw new DScabiException("config is not set", "CRN.RUN.1");
			log.debug("config is not set");
		}
		if (null == m_computeSync) {
			//throw new DScabiException("computeSync is not set", "CRN.RUN.1");
			log.debug("computeSync is not set");
		}
		synchronized (m_computeSync) {
		
			try {
				if (ComputeConfig.OBJECT == m_config.getConfigType()) {
					log.debug("Executing Object");
					m_computeSync.executeObject(m_config.getComputeUnit());
				}
			} catch (Exception e) {
				log.debug("Exception : {}", e.toString());
			}
		
		}
		
	}
	*/
	
	public void doRun() throws IOException, DScabiException {
		
		if (null == m_config) {
			log.debug("doRun() config is not set");
			throw new DScabiException("doRun() config is not set", "CRN.DRN.1");
			//return;
		}
		if (null == m_computeSync) {
			log.debug("doRun() computeSync is not set");
			throw new DScabiException("computeSync is not set", "CRN.DRN.2");
			//return;
		}
		
		int splitno = m_SU;
		log.debug("doRun() splitno : {}", splitno);
		String result = null;
		try {
			if (m_config.isJarFilePathListSet()) {
				log.debug("doRun() isJarFilePathSet() is true");
				m_computeSync.setJarFilePathFromList(m_config.getJarFilePathList());
			}
			m_computeSync.setInput(m_config.getInput());
			if (m_config.isComputeUnitJarsSet()) {
				log.debug("doRun() isComputeUnitJarsSet() is true");
				m_computeSync.setComputeUnitJars(m_config.getComputeUnitJars());
			}
			if (DComputeConfig.OBJECT == m_config.getConfigType()) {
				log.debug("doRun() Executing for Object");
				result = m_computeSync.executeObject(m_config.getComputeUnit());
				log.debug("doRun() result is : {}", result);
			} else if (DComputeConfig.CLASS == m_config.getConfigType()){
				log.debug("doRun() Executing for Class");
				result = m_computeSync.executeClass(m_config.getComputeClass());
				log.debug("doRun() result is : {}", result);
				
			} else if (DComputeConfig.CODE == m_config.getConfigType()){
				log.debug("doRun() Executing for Code");
				result = m_computeSync.executeCode(m_config.getComputeCode());
				log.debug("doRun() result is : {}", result);
			} else if (DComputeConfig.CLASSNAMEINJAR == m_config.getConfigType()){
				log.debug("doRun() Executing for Class Name In Jar");
				result = m_computeSync.executeClassNameInJar(m_config.getJarFilePath(), m_config.getClassNameInJar());
			} else {
				throw new DScabiException("Unknown ComputeConfig type m_config.getConfigType() " + m_config.getConfigType(), "CRN.DRN.3");
			}
			
			synchronized (m_config) {
				log.debug("doRun() Setting result for splitno : {}", splitno);
				m_config.setResult(splitno, result);
				// Not used m_config.setSplitStatus(splitno, true);
				// Not used m_config.incIfExistsFailedSplitRetryMap(splitno);
			}
				
		} catch (ClientProtocolException | SocketException | NoHttpResponseException e) {
			log.debug("doRun() Exception : {}", e.toString());
			// m_computeSync is faulty only in the case of ClientProtocolException/NetworkException
			m_computeSync.setFaulty(true);
			String errorJson = DMJson.error(DMUtil.clientErrMsg(e));	
			synchronized (m_config) {
				// Not used m_config.setOrIncFailedSplitRetryMap(splitno);
				// Not used m_config.setSplitStatus(splitno, false);
				m_config.setResult(splitno, errorJson);
			}
			// m_isError is set only in the case of network exception
			// only in this case retry has to be attempted if maxRetry > 0 is set by User
			m_isError = true; 
		}
		
	}

	public void run() {
		m_isDone = false;
		if (true == m_isError)
			m_retriesTillNow = m_retriesTillNow + 1;
		m_isError = false;
		synchronized (m_computeSync) {
			try {
				m_computeSync.setTU(m_TU);
				m_computeSync.setSU(m_SU);

		        doRun();
		    } catch (Throwable e) {
				log.debug("run() Throwable : {}", e.toString());
				// m_computeSync is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled in doRun() method
				// // // m_computeSync.setFaulty(true);
				
				// TODO: check later, low priority, whether to use m_SU
				int splitno = m_computeSync.getSU(); // just to be exact, getting SU directly from m_computeSync
				log.debug("run() m_computeSync.getSU() : {}", splitno);
				String errorJson = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					// Not used m_config.setOrIncFailedSplitRetryMap(splitno);
					// Not used m_config.setSplitStatus(splitno, false);
					m_config.setResult(splitno, errorJson);
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
