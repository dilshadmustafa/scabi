/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 29-Feb-2016
 * File Name : DComputeAsyncRun_D1.java
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

package com.dilmus.dilshad.scabi.core.D1;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.ParseException;
import org.apache.http.ProtocolException;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.compute.DComputeAsyncConfig;
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;

/**
 * @author Dilshad Mustafa
 *
 */

// Lock order inside transaction : CR, CNB, CC

public class DComputeAsyncRun_D1 implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DComputeAsyncRun_D1.class);
	private DComputeAsyncConfig m_config = null;
	private DComputeNoBlock m_computeNB = null;
	private boolean m_isDone = false;
	private boolean m_isError = false;
	private boolean m_isRetrySubmitted = false;
	private boolean m_isRunOnce = false;
	
	private boolean m_isExecutionError = false;
	
	private long m_TU = 0;
	private long m_SU = 0;
	private int m_retriesTillNow = 0;
	private int m_maxRetry = 0;
	
	private Future<HttpResponse> m_futureHttpResponse = null;
	
	private String m_taskId = null;
	
	String getTaskId() {
		log.debug("getTaskId() m_SU : {}", m_SU);

		if (m_taskId != null)
			return m_taskId;
		
		if (null == m_config) {
			log.debug("getTaskId() m_config is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_config is not set. m_SU : " + m_SU, "CRN.GTD.1"));			
			// Not used return;
		}
		if (0 == m_TU) {
			log.debug("getTaskId() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "CRN.GTD.1"));			
			// Not used return;
		}
		if (0 == m_SU) {
			log.debug("getTaskId() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "CRN.GTD.1"));			
			// Not used return;
		}
		
		if (null == m_taskId)
			m_taskId = m_config.getConfigId() + "_" + m_TU + "_" + m_SU;

		return m_taskId;
	}
	
	public DComputeAsyncRun_D1() {
		m_config = null;
		m_computeNB = null;

		m_isDone = false;
		m_isError = false;
		m_isRetrySubmitted = false;
		m_isRunOnce = false;
		
		m_isExecutionError = false;
		
		m_futureHttpResponse = null;
		
		m_taskId = null;
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

	public Future<HttpResponse> getFutureHttpResponse() {
		return m_futureHttpResponse;
	}
	
	public int setError(boolean status) {
		m_isError = status;
		return 0;
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
	
	public int clearFutureHttpResponse() {
		// Use with caution
		// two concurrent threads accessing m_futureHttpResponse, RetryMonitor, RangeRunner

		m_futureHttpResponse = null;
		return 0;
	}
	
	public int setComputeNB(DComputeNoBlock computeNB) {
		m_computeNB = computeNB;
		return 0;
	}
	
	public DComputeNoBlock getComputeNB() {
		return m_computeNB;
	}

	public int setConfig(DComputeAsyncConfig config) {
		m_config = config;
		return 0;
	}
	
	public DComputeAsyncConfig getConfig() {
		return m_config;
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
	
	public int setFlagsAfterExecutionError() {
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
		
		/* Previous works
		if (null == m_config) {
			// Not used throw new DScabiException("config is not set", "CRN.RUN.1");
			log.debug("doRun() m_config is not set");
			return;
		}
		if (null == m_computeNB) {
			// Not used throw new DScabiException("computeSync is not set", "CRN.RUN.1");
			log.debug("doRun() m_computeNB is not set");
			return;
		}
		*/
		
		// Previous works long splitno = m_SU;
		m_futureHttpResponse = null;
		
		if (m_computeNB.isFaulty()) {
			log.debug("doRun() m_computeNB is marked as faulty. crun m_SU : {}", m_SU);
			String errorJsonStr = DMJson.error("m_computeNB is marked as faulty. crun m_SU : " + m_SU);
			synchronized (m_config) {
				m_config.setResult(m_SU, errorJsonStr);
			}
			m_isError = true; 
			
			m_isDone = true;
			m_isRetrySubmitted = false;
			if (false == m_isRunOnce) {
				m_isRunOnce = true;
			}
			return;
		}
		
		try {
			if (m_config.isJarFilePathListSet()) {
				log.debug("doRun() isJarFilePathSet() is true");
				m_computeNB.setJarFilePathFromList(m_config.getJarFilePathList());
			}
			m_computeNB.setInput(m_config.getInput());
			m_computeNB.setJobId(m_config.getJobId());
			m_computeNB.setConfigId(m_config.getConfigId());
			if (m_config.isComputeUnitJarsSet()) {
				log.debug("doRun() isComputeUnitJarsSet() is true");
				m_computeNB.setComputeUnitJars(m_config.getComputeUnitJars());
			}
			if (DComputeAsyncConfig.OBJECT == m_config.getConfigType()) {
				log.debug("doRun() Executing for Object");
				m_futureHttpResponse = m_computeNB.executeObject(m_config.getComputeUnit());
				
			} else if (DComputeAsyncConfig.CLASS == m_config.getConfigType()){
				log.debug("doRun() Executing for Class");
				m_futureHttpResponse = m_computeNB.executeClass(m_config.getComputeClass());
					
			} else if (DComputeAsyncConfig.CODE == m_config.getConfigType()){
				log.debug("doRun() Executing for Code");
				m_futureHttpResponse = m_computeNB.executeCode(m_config.getComputeCode());
				
			} else if (DComputeAsyncConfig.CLASSNAMEINJAR == m_config.getConfigType()){
				log.debug("doRun() Executing for Class Name In Jar");
				m_futureHttpResponse = m_computeNB.executeClassNameInJar(m_config.getClassNameInJarFilePath(), m_config.getClassNameInJar());
				
			} else {
				throw new DScabiException("Unknown ComputeConfig type m_config.getConfigType() " + m_config.getConfigType(), "CRN.DRN.1");
			}
					
		} catch (ClientProtocolException | IllegalStateException | SocketException | NoHttpResponseException e) {
			log.debug("doRun() Exception : {}", e.toString());
			// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException
			m_computeNB.setFaulty(true);
			String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
			synchronized (m_config) {
				m_config.setResult(m_SU, errorJsonStr);
			}
			// m_isError is set only in the case of network exception
			// only in this case retry has to be attempted if maxRetry > 0 is set by User
			m_isError = true; 
			
			m_isDone = true;
			m_isRetrySubmitted = false;
			if (false == m_isRunOnce) {
				m_isRunOnce = true;
			}

		} // try-catch
		
	}

	public void run() {

		synchronized(this) {
		
		log.debug("run() m_SU : {}", m_SU);

		if (null == m_config) {
			log.debug("run() m_config is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_config is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
			// Not used return;
		}
		if (null == m_computeNB) {
			log.debug("run() m_computeNB is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_computeNB is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
			// Not used return;
		}
		if (0 == m_TU) {
			log.debug("run() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
			// Not used return;
		}
		if (0 == m_SU) {
			log.debug("run() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
			// Not used return;
		}
		
		if (null == m_taskId)
			m_taskId = m_config.getConfigId() + "_" + m_TU + "_" + m_SU;
		
		m_isDone = false;
		if (true == m_isError)
			m_retriesTillNow = m_retriesTillNow + 1;
		m_isError = false;

		synchronized (m_computeNB) {
			try {
				m_computeNB.setTU(m_TU);
				m_computeNB.setSU(m_SU);
				m_computeNB.setTaskId(m_taskId);
				m_computeNB.setConfig(m_config);
		        doRun();
		    } catch (Throwable e) {
				log.debug("run() Throwable : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled in doRun() method
				// // // m_computeNB.setFaulty(true);
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					m_config.setResult(m_SU, errorJsonStr);
				}
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}

		    }
			
		} // End synchronized m_computeNB

		} // End synchronized crun
	}
	
	void get() {
		
		synchronized(this) {
		
		HttpResponse httpResponse = null;
		String result = null;
		
		log.debug("get() m_SU : {}", m_SU);
		
		// if m_isError is already set to true by doRun(), just return
		if (true == m_isError) {
			log.debug("get() m_isError is true for crun m_SU : {}", m_SU);
			return;
		}
		
		// if m_isDone is already set to true by doRun(), just return
		if (true == m_isDone) {
			log.debug("get() m_isDone is true for crun m_SU : {}", m_SU);
			return;
		}

		// Not needed synchronized (m_computeNB) {
		
		if (m_futureHttpResponse != null) {

			try {
				httpResponse = DComputeNoBlock.get(m_futureHttpResponse);
				result = DComputeNoBlock.getResult(httpResponse);
				log.debug("get() splitno m_SU : {}, result : {}", m_SU, result);
				
				m_computeNB.decCountRequests();
	
				synchronized (m_config) {
					m_config.setResult(m_SU, result);
				}
				
				m_futureHttpResponse = null;
			}
			catch (ExecutionException e) {
				if (e.getCause() != null) {
					if (e.getCause() instanceof ConnectException ||
						e.getCause() instanceof ConnectionClosedException ||
						e.getCause() instanceof IllegalStateException ||
						e.getCause() instanceof SocketException ||
						e.getCause() instanceof NoHttpResponseException ||
						e.getCause() instanceof IOException) {
						log.debug("get() e.getCause() : {}", e.getCause().toString());
						log.debug("get() Exception : {}", e.toString());
						// computeNB is faulty only in the case of Network Exception/ConnectException
						m_computeNB.setFaulty(true);

						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						// m_isError is set only in the case of network exception
						// only in this case retry has to be attempted if maxRetry > 0 is set by User
						m_isError = true; 
						
						m_computeNB.decCountRequests();
		
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}
		
						m_futureHttpResponse = null;
								
					} else {
						log.debug("get() ExecutionException : {}", e.toString());
						// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
						// is already handled above
						// // // m_computeNB.setFaulty(true);
						
						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						
						m_computeNB.decCountRequests();

						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}

						m_futureHttpResponse = null;

					} // End if
				} else {
					log.debug("get() ExecutionException : {}", e.toString());
					// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
					// is already handled above
					// // // m_computeNB.setFaulty(true);
					
					String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
					
					m_computeNB.decCountRequests();

					synchronized (m_config) {
						m_config.appendResult(m_SU, errorJsonStr);
					}

					m_futureHttpResponse = null;

				} // End if
		
			} 
			catch (InterruptedException | TimeoutException | ParseException | IOException e) {
				//e.printStackTrace();
				log.debug("get() Exception : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
				// // // m_computeNB.setFaulty(true);
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				
				m_computeNB.decCountRequests();
		
				synchronized (m_config) {
					m_config.appendResult(m_SU, errorJsonStr);
				}
		
				m_futureHttpResponse = null;
						
			} // try-catch
		
		} else {
			//log.debug("get() Inside else block (null == futureHttpResponse)");
			String errorJsonStr = DMJson.error("DComputeAsyncRun:get() Client Side Issue : get() crun futureHttpResponse is null. Split no. : " + m_SU);
			
			m_computeNB.decCountRequests();
			
			synchronized (m_config) {
				//if (false == config.isResultSet(crun.getSU()))
					m_config.appendResult(m_SU, errorJsonStr);
			}
			// if futureHttpResponse is null, exception should have been caught in crun.run() above
			// TODO check if setError() on crun here is needed to enable retry for this crun
		
		} // End if
		
		// Not needed } // End synchronized m_computeNB
		
		m_isDone = true;
		m_isRetrySubmitted = false;
		if (false == m_isRunOnce) {
			m_isRunOnce = true;
		}
		
		} // End synchronized crun
	}
	
}
