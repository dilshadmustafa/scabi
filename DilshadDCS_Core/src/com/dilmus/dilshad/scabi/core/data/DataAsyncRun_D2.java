/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 29-Feb-2016
 * File Name : DataAsyncRun_D2.java
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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.ParseException;
//import org.apache.http.ProtocolException;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;

/**
 * @author Dilshad Mustafa
 *
 */

// Lock order inside transaction : CR, CNB, CC

public class DataAsyncRun_D2 {

	private final Logger log = LoggerFactory.getLogger(DataAsyncRun_D2.class);
	private IConfig m_config = null;
	private DataNoBlock m_computeNB = null;
	private boolean m_isDone = false;
	private boolean m_isError = false;
	private boolean m_isRetrySubmitted = false;
	private boolean m_isRunOnce = false;
	
	private boolean m_isExecutionError = false;
	
	private long m_TU = 0;
	private long m_SU = 0;
	private int m_retriesTillNow = 0;
	private int m_maxRetry = 0;
	
	private String m_taskId = null;
	
	private HashMap<String, DataAsyncConfigNode> m_commandMap = null;
	
	private long m_startCommandId = -1;
	private long m_endCommandId = -1;
	
	public void initialize() {
		
		m_isDone = false;
		m_isError = false;
		m_isRetrySubmitted = false;
		m_isRunOnce = false;
	
		m_isExecutionError = false;
		m_retriesTillNow = 0;
		
		m_taskId = null;
		
		m_startCommandId = -1;
		m_endCommandId = -1;		
	}
	
	String getTaskId() {
		log.debug("getTaskId() m_SU : {}", m_SU);

		if (m_taskId != null)
			return m_taskId;
		
		if (null == m_config) {
			log.debug("getTaskId() m_config is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_config is not set. m_SU : " + m_SU, "CRN.GTD.1"));			
		}
		if (0 == m_TU) {
			log.debug("getTaskId() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "CRN.GTD.1"));			
		}
		if (0 == m_SU) {
			log.debug("getTaskId() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "CRN.GTD.1"));			
		}
		if (m_startCommandId <= 0) {
			log.debug("getTaskId() m_startCommandId is not set. m_startCommandId : {}", m_startCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is not set. m_startCommandId : " + m_startCommandId, "CRN.RUN.1"));			
		}
		if (m_endCommandId <= 0) {
			log.debug("getTaskId() m_endCommandId is not set. m_endCommandId : {}", m_endCommandId);
			throw new RuntimeException(new DScabiException("m_endCommandId is not set. m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (m_startCommandId > m_endCommandId) {
			log.debug("getTaskId() m_startCommandId is greater than m_endCommandId. m_startCommandId : {}, m_endCommandId : {}", m_startCommandId, m_endCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is greater than m_endCommandId. m_startCommandId : " + m_startCommandId + " m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (null == m_taskId)
			m_taskId = m_config.getConfigId() + "_" + m_TU + "_" + m_SU + "_CMDID_" + m_startCommandId + "_" + m_endCommandId;

		return m_taskId;
	}
	
	public DataAsyncRun_D2() {
		m_config = null;
		m_computeNB = null;

		m_isDone = false;
		m_isError = false;
		m_isRetrySubmitted = false;
		m_isRunOnce = false;
		
		m_isExecutionError = false;
		
		m_taskId = null;
		
		m_commandMap = null;
		
		m_startCommandId = -1;
		m_endCommandId = -1;
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

	public int setCommandIdRange(long startCommandId, long endCommandId) {
		m_startCommandId = startCommandId;
		m_endCommandId = endCommandId;
		return 0;
	}
	
	public int clearCommandIdRange() {
		m_startCommandId = -1;
		m_endCommandId = -1;
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
	
	public int setComputeNB(DataNoBlock computeNB) {
		m_computeNB = computeNB;
		return 0;
	}
	
	public DataNoBlock getComputeNB() {
		return m_computeNB;
	}

	public int setCommandMap(HashMap<String, DataAsyncConfigNode> commandMap) {
		m_commandMap = commandMap;
		return 0;
	}
	
	public int setConfig(IConfig config) {
		m_config = config;
		return 0;
	}
	
	public IConfig getConfig() {
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
	
//==============================================================================================================	
	
	public void submitTask() {
		Future<HttpResponse> futureHttpResponse = null;
		
		synchronized(this) {
			
		log.debug("submitTask() m_SU : {}", m_SU);

		if (null == m_config) {
			log.debug("submitTask() m_config is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_config is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (null == m_computeNB) {
			log.debug("submitTask() m_computeNB is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_computeNB is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (0 == m_TU) {
			log.debug("submitTask() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (0 == m_SU) {
			log.debug("submitTask() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (m_startCommandId <= 0) {
			log.debug("submitTask() m_startCommandId is not set. m_startCommandId : {}", m_startCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is not set. m_startCommandId : " + m_startCommandId, "CRN.RUN.1"));			
		}
		if (m_endCommandId <= 0) {
			log.debug("submitTask() m_endCommandId is not set. m_endCommandId : {}", m_endCommandId);
			throw new RuntimeException(new DScabiException("m_endCommandId is not set. m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (m_startCommandId > m_endCommandId) {
			log.debug("submitTask() m_startCommandId is greater than m_endCommandId. m_startCommandId : {}, m_endCommandId : {}", m_startCommandId, m_endCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is greater than m_endCommandId. m_startCommandId : " + m_startCommandId + " m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (null == m_taskId)
			m_taskId = m_config.getConfigId() + "_" + m_TU + "_" + m_SU + "_CMDID_" + m_startCommandId + "_" + m_endCommandId;
		
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

				if (m_computeNB.isFaulty()) {
					log.debug("submitTask() m_computeNB is marked as faulty. crun m_SU : {}", m_SU);
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
				
				/* Previous works
				if (m_config.isJarFilePathListSet()) {
					log.debug("submitRequest() isJarFilePathSet() is true");
					m_computeNB.setJarFilePathFromList(m_config.getJarFilePathList());
				}
				*/
				
				m_computeNB.setInput(m_config.getInput());
				m_computeNB.setAppName(m_config.getAppName());
				m_computeNB.setAppId(m_config.getAppId());
				m_computeNB.setJobId(m_config.getJobId());
				m_computeNB.setConfigId(m_config.getConfigId());
				m_computeNB.setCommandIdRange(m_startCommandId, m_endCommandId);
				m_computeNB.setRetryNumber(m_retriesTillNow);
				m_computeNB.setMaxRetry(m_maxRetry);
				
				/* Previous works
				if (m_config.isComputeUnitJarsSet()) {
					log.debug("submitRequest() isComputeUnitJarsSet() is true");
					m_computeNB.setComputeUnitJars(m_config.getComputeUnitJars());
				}
				*/
				
				if (m_config instanceof DataUnitConfig) {
					log.debug("submitTask() Executing for DataUnit config");
					futureHttpResponse = m_computeNB.executeForDataUnitOperators(m_commandMap);
				} else if (m_config instanceof DMPartitionerConfig) {
					log.debug("submitTask() Executing for Partitioner config");
					// TODO futureHttpResponse = m_computeNB.executeForPartitioner(m_commandMap);
				} else {
					throw new DScabiException("Unknown Config type m_config.getConfigType() " + m_config.getConfigType(), "CRN.DRN.1");
				}
			
		    } catch (ClientProtocolException | IllegalStateException | SocketException | NoHttpResponseException e) {
				log.debug("submitTask() Exception : {}", e.toString());
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

			} catch (Throwable e) {
				log.debug("submitTask() Throwable : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					m_config.setResult(m_SU, errorJsonStr);
				}
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}

		    } // End try-catch
			
		} // End synchronized m_computeNB
		
		getForSubmitTask(futureHttpResponse);
		
		} // End synchronized crun
		
	}
	
	void getForSubmitTask(Future<HttpResponse> futureHttpResponse) {
		
		synchronized(this) {
		
		HttpResponse httpResponse = null;
		String result = null;
		
		log.debug("getForSubmitTask() m_SU : {}", m_SU);
		
		// if m_isError is already set to true, just return
		if (true == m_isError) {
			log.debug("getForSubmitTask() m_isError is true for crun m_SU : {}", m_SU);
			return;
		}
		
		// if m_isDone is already set to true, just return
		if (true == m_isDone) {
			log.debug("getForSubmitTask() m_isDone is true for crun m_SU : {}", m_SU);
			return;
		}

		// Not needed synchronized (m_computeNB) {
		
		if (futureHttpResponse != null) {

			try {
				httpResponse = DataNoBlock.get(futureHttpResponse);
				result = DataNoBlock.getResult(httpResponse);
				log.debug("getForSubmitTask() splitno m_SU : {}, result : {}", m_SU, result);
				
				synchronized (m_config) {
					m_config.setResult(m_SU, result);
				}
			}
			catch (ExecutionException e) {
				if (e.getCause() != null) {
					if (e.getCause() instanceof ConnectException ||
						e.getCause() instanceof ConnectionClosedException ||
						e.getCause() instanceof IllegalStateException ||
						e.getCause() instanceof SocketException ||
						e.getCause() instanceof NoHttpResponseException ||
						e.getCause() instanceof IOException) {
						log.debug("getForSubmitTask() e.getCause() : {}", e.getCause().toString());
						log.debug("getForSubmitTask() Exception : {}", e.toString());
						// computeNB is faulty only in the case of Network Exception/ConnectException
						m_computeNB.setFaulty(true);

						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						// m_isError is set only in the case of network exception
						// only in this case retry has to be attempted if maxRetry > 0 is set by User
						m_isError = true; 
						
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}
					
						m_isDone = true;
						m_isRetrySubmitted = false;
						if (false == m_isRunOnce) {
							m_isRunOnce = true;
						}
								
					} else {
						log.debug("getForSubmitTask() ExecutionException : {}", e.toString());
						// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
						// is already handled above
						
						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}
						
						m_isDone = true;
						m_isRetrySubmitted = false;
						if (false == m_isRunOnce) {
							m_isRunOnce = true;
						}

					} // End if
				} else {
					log.debug("getForSubmitTask() ExecutionException : {}", e.toString());
					// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
					// is already handled above
					
					String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));

					synchronized (m_config) {
						m_config.appendResult(m_SU, errorJsonStr);
					}

					m_isDone = true;
					m_isRetrySubmitted = false;
					if (false == m_isRunOnce) {
						m_isRunOnce = true;
					}

				} // End if
		
			} 
			catch (InterruptedException | TimeoutException | ParseException | IOException e) {
				//e.printStackTrace();
				log.debug("getForSubmitTask() Exception : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
			
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				
				synchronized (m_config) {
					m_config.appendResult(m_SU, errorJsonStr);
				}
			
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}
						
			} // try-catch
		
		} else {
			//log.debug("getForSubmitTask() Inside else block (null == futureHttpResponse)");
			String errorJsonStr = DMJson.error("DComputeAsyncRun:getForSubmitTask() Client Side Issue : getForSubmitTask() crun futureHttpResponse is null. Split no. : " + m_SU);
					
			synchronized (m_config) {
				//if (false == config.isResultSet(crun.getSU()))
					m_config.appendResult(m_SU, errorJsonStr);
			}
			// if futureHttpResponse is null, exception should have been caught in crun.submitTask() above
			// TODO check if setError() on crun here is needed to enable retry for this crun
			
			m_isDone = true;
			m_isRetrySubmitted = false;
			if (false == m_isRunOnce) {
				m_isRunOnce = true;
			}
			
		} // End if
		
		// Not needed } // End synchronized m_computeNB
	
		} // End synchronized crun
	}	
	
//==============================================================================================================	

	public int submitIsDoneProcessing() {
		
		// 1 - done
		// 0 - not done
		// -1 - server side error
		// -2 - client side error
		int ret = -2;
		Future<HttpResponse> futureHttpResponse = null;
		
		synchronized(this) {
			
		log.debug("submitIsDoneProcessing() m_SU : {}", m_SU);

		if (null == m_config) {
			log.debug("submitIsDoneProcessing() m_config is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_config is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (null == m_computeNB) {
			log.debug("submitIsDoneProcessing() m_computeNB is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_computeNB is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (0 == m_TU) {
			log.debug("submitIsDoneProcessing() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (0 == m_SU) {
			log.debug("submitIsDoneProcessing() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (m_startCommandId <= 0) {
			log.debug("submitIsDoneProcessing() m_startCommandId is not set. m_startCommandId : {}", m_startCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is not set. m_startCommandId : " + m_startCommandId, "CRN.RUN.1"));			
		}
		if (m_endCommandId <= 0) {
			log.debug("submitIsDoneProcessing() m_endCommandId is not set. m_endCommandId : {}", m_endCommandId);
			throw new RuntimeException(new DScabiException("m_endCommandId is not set. m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (m_startCommandId > m_endCommandId) {
			log.debug("submitIsDoneProcessing() m_startCommandId is greater than m_endCommandId. m_startCommandId : {}, m_endCommandId : {}", m_startCommandId, m_endCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is greater than m_endCommandId. m_startCommandId : " + m_startCommandId + " m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (null == m_taskId)
			m_taskId = m_config.getConfigId() + "_" + m_TU + "_" + m_SU + "_CMDID_" + m_startCommandId + "_" + m_endCommandId;
		
		// if m_isError is already set to true, just return
		if (true == m_isError) {
			log.debug("submitIsDoneProcessing() m_isError is true for crun m_SU : {}", m_SU);
			return -1;
		}
		
		// if m_isDone is already set to true, just return
		if (true == m_isDone) {
			log.debug("submitIsDoneProcessing() m_isDone is true for crun m_SU : {}", m_SU);
			return -2;
		}		
		
		synchronized (m_computeNB) {
			try {
				m_computeNB.setTU(m_TU);
				m_computeNB.setSU(m_SU);
				m_computeNB.setTaskId(m_taskId);
				m_computeNB.setConfig(m_config);

				if (m_computeNB.isFaulty()) {
					log.debug("submitIsDoneProcessing() m_computeNB is marked as faulty. crun m_SU : {}", m_SU);
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
					return -1;
				}
				
				m_computeNB.setInput(m_config.getInput());
				m_computeNB.setAppName(m_config.getAppName());
				m_computeNB.setAppId(m_config.getAppId());				
				m_computeNB.setJobId(m_config.getJobId());
				m_computeNB.setConfigId(m_config.getConfigId());
				
				log.debug("submitIsDoneProcessing() Executing for isDoneProcessing()");
				futureHttpResponse = m_computeNB.isDoneProcessing();
				ret = 0;
			
		    } catch (ClientProtocolException | IllegalStateException | SocketException | NoHttpResponseException e) {
				log.debug("submitIsDoneProcessing() Exception : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException
				m_computeNB.setFaulty(true);
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					m_config.setResult(m_SU, errorJsonStr);
				}
				// m_isError is set only in the case of network exception
				// only in this case retry has to be attempted if maxRetry > 0 is set by User
				m_isError = true; 
			
				ret = -1;

				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}
				
			} catch (Throwable e) {
				log.debug("submitIsDoneProcessing() Throwable : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					m_config.setResult(m_SU, errorJsonStr);
				}
				
				ret = -2;

				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}
				
		    } // End try-catch
			
		} // End synchronized m_computeNB
		
		log.debug("submitIsDoneProcessing() - 1 - ret : {} for m_SU : {}", ret, m_SU);
		if (0 == ret) {
			ret = getForIsDoneProcessing(futureHttpResponse);
			log.debug("submitIsDoneProcessing() - 2 - ret : {} for m_SU : {}", ret, m_SU);
		}
		
		} // End synchronized crun
	
		return ret;
	}
	
	int getForIsDoneProcessing(Future<HttpResponse> futureHttpResponse) {
		
		// 1 - done
		// 0 - not done
		// -1 - server side error
		// -2 - client side error
		int ret = -2;
	
		synchronized(this) {
		
		HttpResponse httpResponse = null;
		String result = null;
		
		log.debug("getForIsDoneProcessing() m_SU : {}", m_SU);
		
		// if m_isError is already set to true, just return
		if (true == m_isError) {
			log.debug("getForIsDoneProcessing() m_isError is true for crun m_SU : {}", m_SU);
			return -1;
		}
		
		// if m_isDone is already set to true, just return
		if (true == m_isDone) {
			log.debug("getForIsDoneProcessing() m_isDone is true for crun m_SU : {}", m_SU);
			return -2;
		}

		// Not needed synchronized (m_computeNB) {
		
		if (futureHttpResponse != null) {

			try {
				httpResponse = DataNoBlock.get(futureHttpResponse);
				result = DataNoBlock.getResult(httpResponse);
				log.debug("getForIsDoneProcessing() splitno m_SU : {}, result : {}", m_SU, result);
				
				synchronized (m_config) {
					m_config.setResult(m_SU, result);
				}
				
				if (result.contains("{ \"True\" : \"1\" }")) {
					ret = 1;
					log.debug("getForIsDoneProcessing() splitno m_SU : {}, returned result is True:1", m_SU);
				}	
				else if (result.contains("{ \"False\" : \"1\" }")) {
					ret = 0;
					log.debug("getForIsDoneProcessing() splitno m_SU : {}, returned result is False:1", m_SU);
				}
				else {
					ret = -1;
					log.debug("getForIsDoneProcessing() splitno m_SU : {}, returned result is not True:1 or False:1", m_SU);
					m_isDone = true;
					m_isRetrySubmitted = false;
					if (false == m_isRunOnce) {
						m_isRunOnce = true;
					}
				}
			}
			catch (ExecutionException e) {
				if (e.getCause() != null) {
					if (e.getCause() instanceof ConnectException ||
						e.getCause() instanceof ConnectionClosedException ||
						e.getCause() instanceof IllegalStateException ||
						e.getCause() instanceof SocketException ||
						e.getCause() instanceof NoHttpResponseException ||
						e.getCause() instanceof IOException) {
						log.debug("getForIsDoneProcessing() e.getCause() : {}", e.getCause().toString());
						log.debug("getForIsDoneProcessing() Exception : {}", e.toString());
						// computeNB is faulty only in the case of Network Exception/ConnectException
						m_computeNB.setFaulty(true);

						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						// m_isError is set only in the case of network exception
						// only in this case retry has to be attempted if maxRetry > 0 is set by User
						m_isError = true; 
						
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}
						
						ret = -1;
						
						m_isDone = true;
						m_isRetrySubmitted = false;
						if (false == m_isRunOnce) {
							m_isRunOnce = true;
						}
								
					} else {
						log.debug("getForIsDoneProcessing() ExecutionException : {}", e.toString());
						// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
						// is already handled above
						
						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}

						ret = -2;
						
						m_isDone = true;
						m_isRetrySubmitted = false;
						if (false == m_isRunOnce) {
							m_isRunOnce = true;
						}

					} // End if
				} else {
					log.debug("getForIsDoneProcessing() ExecutionException : {}", e.toString());
					// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
					// is already handled above
					
					String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
					
					synchronized (m_config) {
						m_config.appendResult(m_SU, errorJsonStr);
					}

					ret = -2;
					
					m_isDone = true;
					m_isRetrySubmitted = false;
					if (false == m_isRunOnce) {
						m_isRunOnce = true;
					}

				} // End if
		
			} 
			catch (InterruptedException | TimeoutException | ParseException | IOException e) {
				//e.printStackTrace();
				log.debug("getForIsDoneProcessing() Exception : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				
				synchronized (m_config) {
					m_config.appendResult(m_SU, errorJsonStr);
				}
		
				ret = -2;
				
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}
						
			} // try-catch
		
		} else {
			//log.debug("getForIsDoneProcessing() Inside else block (null == futureHttpResponse)");
			String errorJsonStr = DMJson.error("DComputeAsyncRun:getForIsDoneProcessing() Client Side Issue : getForIsDoneProcessing() crun futureHttpResponse is null. Split no. : " + m_SU);
			
			synchronized (m_config) {
				//if (false == config.isResultSet(crun.getSU()))
					m_config.appendResult(m_SU, errorJsonStr);
			}
			// if futureHttpResponse is null, exception should have been caught in crun.submitIsDoneProcessing() above
			// TODO check if setError() on crun here is needed to enable retry for this crun
		
			ret = -2;
			
			m_isDone = true;
			m_isRetrySubmitted = false;
			if (false == m_isRunOnce) {
				m_isRunOnce = true;
			}
		} // End if
		
		// Not needed } // End synchronized m_computeNB
		
		} // End synchronized crun
		
		return ret;
	}
	
//==============================================================================================================	
	
	public void submitRetrieveResult() {
		Future<HttpResponse> futureHttpResponse = null;
		
		synchronized(this) {
			
		log.debug("submitRetrieveResult() m_SU : {}", m_SU);

		if (null == m_config) {
			log.debug("submitRetrieveResult() m_config is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_config is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (null == m_computeNB) {
			log.debug("submitRetrieveResult() m_computeNB is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_computeNB is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (0 == m_TU) {
			log.debug("submitRetrieveResult() m_TU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_TU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (0 == m_SU) {
			log.debug("submitRetrieveResult() m_SU is not set. m_SU : {}", m_SU);
			throw new RuntimeException(new DScabiException("m_SU is not set. m_SU : " + m_SU, "CRN.RUN.1"));			
		}
		if (m_startCommandId <= 0) {
			log.debug("submitRetrieveResult() m_startCommandId is not set. m_startCommandId : {}", m_startCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is not set. m_startCommandId : " + m_startCommandId, "CRN.RUN.1"));			
		}
		if (m_endCommandId <= 0) {
			log.debug("submitRetrieveResult() m_endCommandId is not set. m_endCommandId : {}", m_endCommandId);
			throw new RuntimeException(new DScabiException("m_endCommandId is not set. m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (m_startCommandId > m_endCommandId) {
			log.debug("submitRetrieveResult() m_startCommandId is greater than m_endCommandId. m_startCommandId : {}, m_endCommandId : {}", m_startCommandId, m_endCommandId);
			throw new RuntimeException(new DScabiException("m_startCommandId is greater than m_endCommandId. m_startCommandId : " + m_startCommandId + " m_endCommandId : " + m_endCommandId, "CRN.RUN.1"));			
		}
		if (null == m_taskId)
			m_taskId = m_config.getConfigId() + "_" + m_TU + "_" + m_SU + "_CMDID_" + m_startCommandId + "_" + m_endCommandId;
		
		// if m_isError is already set to true, just return
		if (true == m_isError) {
			log.debug("submitRetrieveResult() m_isError is true for crun m_SU : {}", m_SU);
			return;
		}
		
		// if m_isDone is already set to true, just return
		if (true == m_isDone) {
			log.debug("submitRetrieveResult() m_isDone is true for crun m_SU : {}", m_SU);
			return;
		}		
		
		synchronized (m_computeNB) {
			try {
				m_computeNB.setTU(m_TU);
				m_computeNB.setSU(m_SU);
				m_computeNB.setTaskId(m_taskId);
				m_computeNB.setConfig(m_config);

				if (m_computeNB.isFaulty()) {
					log.debug("submitRetrieveResult() m_computeNB is marked as faulty. crun m_SU : {}", m_SU);
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
				
				m_computeNB.setInput(m_config.getInput());
				m_computeNB.setAppName(m_config.getAppName());
				m_computeNB.setAppId(m_config.getAppId());
				m_computeNB.setJobId(m_config.getJobId());
				m_computeNB.setConfigId(m_config.getConfigId());
				
				log.debug("submitRetrieveResult() Executing for retrieveResult()");
				futureHttpResponse = m_computeNB.retrieveResult();
			
		    } catch (ClientProtocolException | IllegalStateException | SocketException | NoHttpResponseException e) {
				log.debug("submitRetrieveResult() Exception : {}", e.toString());
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

			} catch (Throwable e) {
				log.debug("submitRetrieveResult() Throwable : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				synchronized (m_config) {
					m_config.setResult(m_SU, errorJsonStr);
				}
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}

		    } // End try-catch
			
		} // End synchronized m_computeNB

		getForRetrieveResult(futureHttpResponse);		
		
		} // End synchronized crun
		
	}
	
	void getForRetrieveResult(Future<HttpResponse> futureHttpResponse) {
		
		synchronized(this) {
		
		HttpResponse httpResponse = null;
		String result = null;
		
		log.debug("getForRetrieveResult() m_SU : {}", m_SU);
		
		// if m_isError is already set to true, just return
		if (true == m_isError) {
			log.debug("getForRetrieveResult() m_isError is true for crun m_SU : {}", m_SU);
			return;
		}
		
		// if m_isDone is already set to true, just return
		if (true == m_isDone) {
			log.debug("getForRetrieveResult() m_isDone is true for crun m_SU : {}", m_SU);
			return;
		}

		// Not needed synchronized (m_computeNB) {
		
		if (futureHttpResponse != null) {

			try {
				httpResponse = DataNoBlock.get(futureHttpResponse);
				result = DataNoBlock.getResult(httpResponse);
				log.debug("getForRetrieveResult() splitno m_SU : {}, result : {}", m_SU, result);
				
				synchronized (m_config) {
					m_config.setResult(m_SU, result);
				}
				
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}
			}
			catch (ExecutionException e) {
				if (e.getCause() != null) {
					if (e.getCause() instanceof ConnectException ||
						e.getCause() instanceof ConnectionClosedException ||
						e.getCause() instanceof IllegalStateException ||
						e.getCause() instanceof SocketException ||
						e.getCause() instanceof NoHttpResponseException ||
						e.getCause() instanceof IOException) {
						log.debug("getForRetrieveResult() e.getCause() : {}", e.getCause().toString());
						log.debug("getForRetrieveResult() Exception : {}", e.toString());
						// computeNB is faulty only in the case of Network Exception/ConnectException
						m_computeNB.setFaulty(true);

						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						// m_isError is set only in the case of network exception
						// only in this case retry has to be attempted if maxRetry > 0 is set by User
						m_isError = true; 
						
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}
		
						m_isDone = true;
						m_isRetrySubmitted = false;
						if (false == m_isRunOnce) {
							m_isRunOnce = true;
						}
								
					} else {
						log.debug("getForRetrieveResult() ExecutionException : {}", e.toString());
						// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
						// is already handled above
						
						String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
						
						synchronized (m_config) {
							m_config.appendResult(m_SU, errorJsonStr);
						}

						m_isDone = true;
						m_isRetrySubmitted = false;
						if (false == m_isRunOnce) {
							m_isRunOnce = true;
						}

					} // End if
				} else {
					log.debug("getForRetrieveResult() ExecutionException : {}", e.toString());
					// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
					// is already handled above
					
					String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
					
					synchronized (m_config) {
						m_config.appendResult(m_SU, errorJsonStr);
					}

					m_isDone = true;
					m_isRetrySubmitted = false;
					if (false == m_isRunOnce) {
						m_isRunOnce = true;
					}

				} // End if
		
			} 
			catch (InterruptedException | TimeoutException | ParseException | IOException e) {
				//e.printStackTrace();
				log.debug("getForRetrieveResult() Exception : {}", e.toString());
				// m_computeNB is faulty only in the case of ClientProtocolException/NetworkException which 
				// is already handled above
				
				String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(e));
				
				synchronized (m_config) {
					m_config.appendResult(m_SU, errorJsonStr);
				}
		
				m_isDone = true;
				m_isRetrySubmitted = false;
				if (false == m_isRunOnce) {
					m_isRunOnce = true;
				}
						
			} // try-catch
		
		} else {
			//log.debug("getForRetrieveResult() Inside else block (null == futureHttpResponse)");
			String errorJsonStr = DMJson.error("DComputeAsyncRun:getForRetrieveResult() Client Side Issue : getForRetrieveResult() crun futureHttpResponse is null. Split no. : " + m_SU);
			
			synchronized (m_config) {
				//if (false == config.isResultSet(crun.getSU()))
					m_config.appendResult(m_SU, errorJsonStr);
			}
			// if futureHttpResponse is null, exception should have been caught in crun.submitRetrieveResult() above
			// TODO check if setError() on crun here is needed to enable retry for this crun
		
			m_isDone = true;
			m_isRetrySubmitted = false;
			if (false == m_isRunOnce) {
				m_isRunOnce = true;
			}
			
		} // End if
		
		// Not needed } // End synchronized m_computeNB
		
		} // End synchronized crun
	}	
	
}
