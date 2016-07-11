/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 26-Feb-2016
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
//import com.dilmus.dilshad.scabi.core.async.DComputeNoBlock;
import com.dilmus.dilshad.scabi.core.async.DComputeNoBlock;

/**
 * @author Dilshad Mustafa
 *
 */

// Lock order inside transaction : CR, C

public class DRetrySyncMonitor implements Runnable {
	private final Logger log = LoggerFactory.getLogger(DRetrySyncMonitor.class);
	
	private DComputeSync m_compute = null;
	private DMeta m_meta = null;
	private ExecutorService m_threadPool = null;
	private LinkedList<DComputeSyncConfig> m_cconfigList = null;
	private LinkedList<DComputeSyncRun> m_crunList = null;
	private LinkedList<DComputeBlock> m_cbList = null;
	
	// Previous works private LinkedList<DComputeBlock> m_cbWorkingList = null;
	// Previous works private LinkedList<DComputeSyncRun> m_localCRunList = null;
	// Previous works private LinkedList<DComputeBlock> m_localCBList = null;
	
	private long m_cbListSize = 0;
	private long m_cbBalancedListSize = 0;
	
	public DRetrySyncMonitor(DComputeSync compute, DMeta meta) {
		m_compute = compute;
		m_meta = meta;
		
		m_cconfigList = compute.getCConfigList();
		m_crunList = compute.getCRunList();
		m_cbList = compute.getCBList();
		m_threadPool = compute.getExecutorService();
		
		// Previous works m_cbWorkingList = new LinkedList<DComputeBlock>();
		// Previous works m_localCRunList = new LinkedList<DComputeSyncRun>();
		// Previous works m_localCBList = new LinkedList<DComputeBlock>();
		
		m_cbListSize = compute.getCBListSize();
		m_cbBalancedListSize = 0;
	}
	
	LinkedList<DComputeBlock> getCBList() {
		return m_cbList;
		// Previous works return m_localCBList;
	}

	private LinkedList<DComputeBlock> balance(LinkedList<DComputeBlock> cba, int splitTotal) {
		
		boolean check = true;
		// Previous works int current = cba.size();
		long current = m_cbListSize;
		LinkedList<DComputeBlock> cbaBalanced = new LinkedList<DComputeBlock>();
		DComputeBlock another = null;
		
		cbaBalanced.addAll(cba);
		m_cbBalancedListSize = m_cbListSize;
		while (check && current < splitTotal) {
			
			for (DComputeBlock cb : cba) {
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
		return cbaBalanced;
	}

	public int doRetry() throws ParseException, IOException, DScabiClientException {
		long fcTotal = 0;
		// Previous works int cbWorkingTotal = 0;
		// Previous works boolean firstTime = true;
		// Previous works boolean isAllRunOnce = true;
		
		// RetryMonitor is started only after all ComputeRun and ComputeBlock are created in Compute class
		// copy m_crunList, m_cbList to local copy, m_localCRunList, m_localCBList
		/* Previous works
		synchronized(m_compute) {
			for (DComputeSyncRun crun : m_crunList) {
				if (false == m_localCRunList.contains(crun))
					m_localCRunList.add(crun);
			}
			for (DComputeBlock cb : m_cbList) {
				if (false == m_localCBList.contains(cb))
					m_localCBList.add(cb);
			}
		}
		*/
		
		while(true) {
			
		fcTotal = 0;
		// Previous works isAllRunOnce = true;
		boolean isDone = true;
		boolean isRetrySubmitted = false;
		
		for (DComputeSyncRun crun : m_crunList /* Previous works m_localCRunList */) {

			synchronized(crun) {
				
			/* Previous works
			if (false == crun.isRunOnce())
				isAllRunOnce = false;
			*/
			if (false == crun.isDone())
				isDone = false;
			if (true == crun.isRetrySubmitted())
				isRetrySubmitted = true;
			
			if (crun.getRetriesTillNow() < crun.getMaxRetry() 
					&& true == crun.isError() && true == crun.isDone() 
					&& false == crun.isRetrySubmitted()) {
				fcTotal = fcTotal + 1;
			}
			
			} // End synchronized crun
		}
		
		//log.debug("doRetry() fcTotal : {}", fcTotal);
		// Previous works log.debug("doRetry() isAllRunOnce : {}", isAllRunOnce);
		//log.debug("doRetry() isDone : {}", isDone);
		//log.debug("doRetry() isRetrySubmitted : {}", isRetrySubmitted);
		
		if (0 == fcTotal && true == isDone && false == isRetrySubmitted) {
			log.debug("doRetry() fcTotal : {}", fcTotal);
			log.debug("doRetry() isDone : {}", isDone);
			log.debug("doRetry() isRetrySubmitted : {}", isRetrySubmitted);
			return 0;
		}

		/* Previous works
		if (0 == fcTotal && true == isAllRunOnce) {
			log.debug("doRetry() fcTotal : {}", fcTotal);
			return 0;
		}
		*/
		
		/* Previous works
		if (firstTime) {
			m_cbWorkingList.addAll(m_localCBList);
			firstTime = false;
		}
		cbWorkingTotal = 0;
		for (DComputeBlock cb : m_cbWorkingList) {
			if (cb.isFaulty()) {
				//m_cbWorkingList.remove(cb);
				//if (false == m_localCBList.contains(cb))
				//		m_localCBList.add(cb);
			} else {
				cbWorkingTotal = cbWorkingTotal + 1;
			}
		}
		
		for (DComputeBlock cb : m_cbWorkingList) {
			if (false == m_localCBList.contains(cb))
				m_localCBList.add(cb);
		}

		for (DComputeBlock cb : m_localCBList) {
			if (m_cbWorkingList.contains(cb) && cb.isFaulty()) {
				m_cbWorkingList.remove(cb);
				log.debug("removing faulty cb");
			}
		}
		*/
		
		//TODO make a faulty cblist
		/*
		if (fcTotal > cbWorkingTotal) {
			try {
				List<DComputeSync> cbaoriginal = m_meta.getComputeManyMayExclude(fcTotal - cbWorkingTotal, m_cbWorkingList);
				List<DComputeSync> cba = null;
				if(cbaoriginal != null) {
					cba = balance(cbaoriginal, fcTotal - cbWorkingTotal);
					for (DComputeSync cb : cba) {
						m_cbWorkingList.add(0, cb); // inserts at the beginning
					}
				}
			} catch (Error | RuntimeException e) {
				// continue with existing cbs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing cbs");
				// //throw e;
			} catch (Exception e) {
				// continue with existing cbs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing cbs");
				// //throw new RuntimeException(e);
			} catch (Throwable e) {
				// continue with existing cbs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing cbs");
				// //throw e;
			}
			
		}
		*/
		
		// Previous works int k = 0;
		ListIterator<DComputeBlock> itr = m_cbList.listIterator();
		for (DComputeSyncRun crun : m_crunList /* Previous works m_localCRunList */) {

			synchronized(crun) {
				
			if (crun.getRetriesTillNow() < crun.getMaxRetry() 
					&& true == crun.isError() && true == crun.isDone() 
					&& false == crun.isRetrySubmitted()) {
				log.debug("Inside split for loop");
				log.debug("crun.isError() : {}", crun.isError());
				log.debug("crun.isDone() : {}", crun.isDone());
				log.debug("crun.isRetrySubmitted() : {}", crun.isRetrySubmitted());
				log.debug("crun.getSU() : {}", crun.getSU());

				// Previous works if (k >= m_cbWorkingList.size())
				// Previous works 	k = 0;
				// Previous works DComputeBlock cb = m_cbWorkingList.get(k);
				DComputeBlock cb = null;
        		boolean check = true;
				long count = 0;
        		while (check) {
	        		if (itr.hasNext()) {
	        			cb = itr.next();
	        			if (cb.isFaulty() == false)
	        				check = false;
	        		}
	                else {
	        			itr = m_cbList.listIterator();
	        			cb = itr.next();
	        			if (cb.isFaulty() == false)
	        				check = false;
	        		}
	        		if (false == check)
	        			break;
	        		count++;
	        		if (count >= m_cbListSize)
	        			throw new DScabiClientException("Unable to find a working CB", "RSM.DRY.1");
				}
				//log.debug("cb.toString() : {}", cb.toString());
				crun.setComputeBlock(cb);
				// Previous works k++;
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
			
			} // End synchronized crun
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
