/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 26-Feb-2016
 * File Name : DRetryMonitor.java
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

7. You should not redistribute this Software and/or any modified works of this 
Software, including its source code and/or its compiled object binary form, under 
differently named or renamed software. You should not publish this Software, including 
its source code and/or its compiled object binary form, modified or original, under 
your name or your company name or your product name. You should not sell this Software 
to any party, organization, company, legal entity and/or individual.

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

package com.dilmus.dilshad.scabi.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.core.async.DComputeNoBlock;

/**
 * @author Dilshad Mustafa
 *
 */
public class DRetryMonitor implements Runnable {
	private final Logger log = LoggerFactory.getLogger(DRetryMonitor.class);
	
	private DCompute m_compute = null;
	private DMeta m_meta = null;
	private ExecutorService m_threadPool = null;
	private List<DComputeConfig> m_cconfigList = null;
	private List<DComputeRun> m_crunList = null;
	private List<DComputeSync> m_csyncList = null;
	private List<DComputeSync> m_csyncWorkingList = null;
	
	private List<DComputeRun> m_localCRunList = null;
	private List<DComputeSync> m_localCSyncList = null;
	
	public DRetryMonitor(DCompute compute, DMeta meta) {
		m_compute = compute;
		m_meta = meta;
		
		m_cconfigList = compute.getCConfigList();
		m_crunList = compute.getCRunList();
		m_csyncList = compute.getCSyncList();
		m_threadPool = compute.getExecutorService();
		
		m_csyncWorkingList = new ArrayList<DComputeSync>();
		
		m_localCRunList = new ArrayList<DComputeRun>();
		m_localCSyncList = new ArrayList<DComputeSync>();
		
	}
	
	List<DComputeSync> getCSyncList() {
		return m_localCSyncList;
	}

	private List<DComputeSync> balance(List<DComputeSync> csynca, int splitTotal) {
		
		boolean check = true;
		int current = csynca.size();
		List<DComputeSync> csyncaBalanced = new ArrayList<DComputeSync>();
		DComputeSync another = null;
		
		csyncaBalanced.addAll(csynca);
		while (check && current < splitTotal) {
			
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
				if (current >= splitTotal) {
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

	public int doRetry() throws ParseException, IOException, DScabiClientException {
		int fcTotal = 0;
		int csyncWorkingTotal = 0;
		boolean firstTime = true;
		boolean isAllRunOnce = true;
		
		// RetryMonitor is started only after all ComputeRun and ComputeSync are created in Compute class
		// copy m_crunList, m_csyncList to local copy, m_localCRunList, m_localCSyncList
		synchronized(m_compute) {
			for (DComputeRun crun : m_crunList) {
				if (false == m_localCRunList.contains(crun))
					m_localCRunList.add(crun);
			}
			for (DComputeSync csync : m_csyncList) {
				if (false == m_localCSyncList.contains(csync))
					m_localCSyncList.add(csync);
			}
		}
		
		while(true) {
			
		fcTotal = 0;
		isAllRunOnce = true;
		for (DComputeRun crun : m_localCRunList) {
			if (false == crun.isRunOnce())
				isAllRunOnce = false;
			if (crun.getRetriesTillNow() < crun.getMaxRetry() 
					&& true == crun.isError() && true == crun.isDone() 
					&& false == crun.isRetrySubmitted()) {
				fcTotal = fcTotal + 1;
			}
		}
		//log.debug("doRetry() isAllRunOnce : {}", isAllRunOnce);
		if (0 == fcTotal && true == isAllRunOnce) {
			log.debug("doRetry() fcTotal : {}", fcTotal);
			return 0;
		}

		if (firstTime) {
			m_csyncWorkingList.addAll(m_localCSyncList);
			firstTime = false;
		}
		csyncWorkingTotal = 0;
		for (DComputeSync csync : m_csyncWorkingList) {
			if (csync.isFaulty()) {
				//m_csyncWorkingList.remove(csync);
				//if (false == m_localCSyncList.contains(csync))
				//		m_localCSyncList.add(csync);
			} else {
				csyncWorkingTotal = csyncWorkingTotal + 1;
			}
		}
		
		for (DComputeSync csync : m_csyncWorkingList) {
			if (false == m_localCSyncList.contains(csync))
				m_localCSyncList.add(csync);
		}

		for (DComputeSync csync : m_localCSyncList) {
			if (m_csyncWorkingList.contains(csync) && csync.isFaulty()) {
				m_csyncWorkingList.remove(csync);
				log.debug("removing faulty csync");
			}
		}

		//TODO make a faulty csynclist
		/*
		if (fcTotal > csyncWorkingTotal) {
			try {
				List<DComputeSync> csyncaoriginal = m_meta.getComputeManyMayExclude(fcTotal - csyncWorkingTotal, m_csyncWorkingList);
				List<DComputeSync> csynca = null;
				if(csyncaoriginal != null) {
					csynca = balance(csyncaoriginal, fcTotal - csyncWorkingTotal);
					for (DComputeSync csync : csynca) {
						m_csyncWorkingList.add(0, csync); // inserts at the beginning
					}
				}
			} catch (Error | RuntimeException e) {
				// continue with existing csyncs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing csyncs");
				// //throw e;
			} catch (Exception e) {
				// continue with existing csyncs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing csyncs");
				// //throw new RuntimeException(e);
			} catch (Throwable e) {
				// continue with existing csyncs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing csyncs");
				// //throw e;
			}
			
		}
		*/
		
		int k = 0;
		for (DComputeRun crun : m_localCRunList) {
			if (crun.getRetriesTillNow() < crun.getMaxRetry() 
					&& true == crun.isError() && true == crun.isDone() 
					&& false == crun.isRetrySubmitted()) {
				log.debug("Inside split for loop");
				log.debug("crun.isError() : {}", crun.isError());
				log.debug("crun.isDone() : {}", crun.isDone());
				log.debug("crun.isRetrySubmitted() : {}", crun.isRetrySubmitted());
				log.debug("crun.getSU() : {}", crun.getSU());

				if (k >= m_csyncWorkingList.size())
					k = 0;
				DComputeSync csync = m_csyncWorkingList.get(k);
				//log.debug("csync.toString() : {}", csync.toString());
				crun.setComputeSync(csync);
				k++;
				crun.setRetrySubmitStatus(true);
				
				synchronized(m_compute) {
					
				// In Compute class, initialize() method, after thread pool shutdown, m_threadPool is set to null
				if (null == m_compute.getExecutorService()) {
					return 0;
				}

				Future<?> f = m_threadPool.submit(crun);
				m_compute.putFutureCRunMap(f, crun);
				// Not used m_compute.addToFutureList(f);
				
				}
			}
			
		}
		
		} // End While
		
		// return 0;
	}
	
	public void run() {
		try {
			doRetry();
		}
		catch (Error | RuntimeException e) {
			//e.printStackTrace();
			throw e;
	    } catch (Exception e) {
	        //e.printStackTrace();
	        throw new RuntimeException(e);
	    } catch (Throwable e) {
	    	//e.printStackTrace();
	        throw new RuntimeException(e);
	    }

	}
}
