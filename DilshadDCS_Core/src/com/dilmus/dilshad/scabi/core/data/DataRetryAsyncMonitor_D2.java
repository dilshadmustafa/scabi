/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 02-Mar-2016
 * File Name : DataRetryAsyncMonitor_D2.java
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
//import java.net.ConnectException;
//import java.util.HashMap;
import java.util.LinkedList;
//import java.util.List;
import java.util.ListIterator;
//import java.util.Set;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeoutException;

//import org.apache.http.ConnectionClosedException;
//import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;

/**
 * @author Dilshad Mustafa
 *
 */

// Lock order inside transaction : CR

public class DataRetryAsyncMonitor_D2 implements Runnable {
	private final Logger log = LoggerFactory.getLogger(DataRetryAsyncMonitor_D2.class);
	
	private Data m_compute = null;
	private DMeta m_meta = null;
	private LinkedList<DataAsyncConfigNode> m_cconfigList = null;
	private LinkedList<DataAsyncRun_D2> m_crunList = null;
	private LinkedList<DataNoBlock> m_cnbList = null;
	
	//Previous works private LinkedList<DComputeNoBlock> m_cnbWorkingList = null;
	//Previous works private LinkedList<DComputeAsyncRun> m_localCRunList = null;
	//Previous works private LinkedList<DComputeNoBlock> m_localCNBList = null;
	
	private LinkedList<DataAsyncRun_D2> m_retryCRunList = null;
	private LinkedList<DataAsyncRun_D2> m_listBlockedCRun = null;
	private LinkedList<DataAsyncRun_D2> m_allowedCRunList = null;
	
	private LinkedList<DataAsyncRun_D2> m_listBlockedCRun2 = null;
	private LinkedList<DataAsyncRun_D2> m_allowedCRunList2 = null;	
	
	private long m_cnbListSize = 0; // TODO change to long
	
	private DMCounter m_gcCounter = new DMCounter();
	
	private void gc() {
		m_gcCounter.inc();
		if (m_gcCounter.value() >= 2500) {
			System.gc();
			m_gcCounter.set(0);
		}
	}	
	
	public DataRetryAsyncMonitor_D2(Data compute, DMeta meta) {
		m_compute = compute;
		m_meta = meta;
		
		m_cconfigList = compute.getConfigNodeList();
		m_crunList = compute.getCRunList();
		m_cnbList = compute.getCNBList();
		
		//Previous works m_cnbWorkingList = new ArrayList<DComputeNoBlock>();
		//Previous works m_localCRunList = new ArrayList<DComputeAsyncRun>();
		//Previous works m_localCNBList = new ArrayList<DComputeNoBlock>();
		
		m_retryCRunList = new LinkedList<DataAsyncRun_D2>();
		
		m_listBlockedCRun = new LinkedList<DataAsyncRun_D2>();
		m_allowedCRunList = new LinkedList<DataAsyncRun_D2>();
		
		m_listBlockedCRun2 = new LinkedList<DataAsyncRun_D2>();
		m_allowedCRunList2 = new LinkedList<DataAsyncRun_D2>();
		
		m_cnbListSize = compute.getCNBListSize();
	}
	
	public LinkedList<DataNoBlock> getCNBList() {
		return m_cnbList;
		// Previous works return m_localCNBList;
	}
	
	private int retryShuffle(int commandId, DataAsyncRun_D2 crun) {
		
		// TODO retryShuffle is an action
		// get the cnb of crun
		// use all crun from crun list, get the cnb of all crun(s) and directly invoke
		// method on the cnb
				
		// TODO this Future class and Future.get() based
		// so submit all retries first and then do the Future.get() part
		
		return 0;
	}
	
	private int retryShuffle(int commandId, DataAsyncRun_D2 errorReporterCrun, DataAsyncRun_D2 errorCrun) {
		
		// TODO retryShuffle is an action
		// get the cnb of crun(s) errorReporterCrun, errorCrun and directly invoke
		// method on the cnb
				
		// TODO this Future class and Future.get() based
		// so submit all retries first and then do the Future.get() part
		
		return 0;
	}
	
	private long timeToWait(long whichTurn) {
		// Use 5000 msec just for debugging, to avoid frequent isDoneProcessing calls
		return 60000;
		
		// TODO uncomment code below before final release
		/* Use this code before final release
		if (whichTurn >= 0 && whichTurn <= 9)
			return 50; // 50 msec
		else if (whichTurn >= 10 && whichTurn <= 19)
			return 100; // 100 msec
		else if (whichTurn >= 20 && whichTurn <= 29)
			return 250; // 250 msec
		else if (whichTurn >= 30 && whichTurn <= 39)
			return 500; // 500 msec
		else if (whichTurn >= 40 && whichTurn <= 49)
			return 750; // 750 msec
		else if (whichTurn >= 50 && whichTurn <= 59)
			return 1000; // 1 sec
		else if (whichTurn >= 60 && whichTurn <= 69)
			return 10*1000; // 10 sec
		else if (whichTurn >= 70 && whichTurn <= 79)
			return 20*1000; // 20 sec
		else if (whichTurn >= 80 && whichTurn <= 89)
			return 30*1000; // 30 sec
		else if (whichTurn >= 90 && whichTurn <= 99)
			return 40*1000; // 40 sec
		else if (whichTurn >= 100 && whichTurn <= 109)
			return 50*1000; // 50 sec
		else if (whichTurn >= 110 && whichTurn <= 119)
			return 60*1000; // 1 min
		else // for all others, even if whichTurn (long) overflows and becomes negative value, wait is 1 minute
			return 60*1000; // 1 min
		*/
	}	
	
	public int executeRetry() {
		
		long time1 = System.currentTimeMillis();
		
		m_listBlockedCRun.clear();
		m_listBlockedCRun.addAll(m_retryCRunList);
		m_allowedCRunList.clear();
		boolean check = true;
		while (check) {
			
			for (DataAsyncRun_D2 crun : m_listBlockedCRun) {
				DataNoBlock cnb = crun.getComputeNB();
				boolean proceed = false;
				synchronized(cnb) {
					if (cnb.isAllowed()) {
						// This cnb incCountRequests() is placed here for a reason so that none can get through max value
						cnb.incCountRequests();
						proceed = true;
					}
				}
				if (proceed) {
					m_allowedCRunList.add(crun);
					synchronized(crun) {
						crun.setRetrySubmitStatus(true);
						crun.submitTask();
					}
					cnb.decCountRequests();
					gc();
				} else
					check = false;

			} // End for
			
			for (DataAsyncRun_D2 crun : m_allowedCRunList) {
				m_listBlockedCRun.remove(crun);
			} // End for
			m_allowedCRunList.clear();
			
			if (check)
				break;
			check = true;
			
		} // End while				
		
//===============================================================================================================				
		// isDoneProcessing, retrieveResult part
		
		boolean isFirstTime = true;
		long time2 = 0;
		long typicalTimeTaken = 5000; //1000;
		
		m_listBlockedCRun2.clear();
		m_listBlockedCRun2.addAll(m_retryCRunList);
		m_allowedCRunList2.clear();
		boolean check2 = true;
		while (check2) {
			
			for (DataAsyncRun_D2 crun : m_listBlockedCRun2) {
				DataNoBlock cnb = crun.getComputeNB();
				boolean proceed = false;
				synchronized(cnb) {
					if (cnb.isAllowed()) {
						// This cnb incCountRequests() is placed here for a reason so that none can get through max value
						cnb.incCountRequests();
						proceed = true;
					}
				}
				if (proceed) {
					m_allowedCRunList2.add(crun);
					int ret = 0;
					long whichTurn = 0;
					
					while (0 == (ret = crun.submitIsDoneProcessing())) {
						log.debug("executeRetry() still inside while loop. crun.getSU() : {}, whichTurn : {}", crun.getSU(), whichTurn);
						try {
							if (isFirstTime) {
								long waitTime = timeToWait(whichTurn);
								log.debug("executeRetry() Going to sleep for waitTime : {}", waitTime);
								Thread.sleep(waitTime);
								whichTurn++;
							}
							else {
								log.debug("executeRetry() Going to sleep for typicalTimeTaken : {}", typicalTimeTaken);							
								Thread.sleep(typicalTimeTaken);
							}
						} catch (InterruptedException e) {
							// TODO further analysis - do I throw exception here?
							throw new RuntimeException(e);
						}
					}
					gc();
					if (1 == ret) {
						if (isFirstTime) {
							time2 = System.currentTimeMillis();
							typicalTimeTaken = time2 - time1;
							log.debug("executeRetry() typicalTimeTaken : {}, crun.getSU() : {}, whichTurn : {}", typicalTimeTaken, crun.getSU(), whichTurn); 
							isFirstTime = false;
						}
						crun.submitRetrieveResult();
					}					
					cnb.decCountRequests();
				} else
					check2 = false;

			} // End for
			
			for (DataAsyncRun_D2 crun : m_allowedCRunList2) {
				m_listBlockedCRun2.remove(crun);
			} // End for
			m_allowedCRunList2.clear();
			
			if (check2)
				break;
			check2 = true;
			
		} // End while			
		
		m_retryCRunList.clear();
		
		return 0;
		
	}
	
	public int doRetry() throws ParseException, IOException, DScabiClientException {
		long fcTotal = 0;
		// Previous works long cnbWorkingTotal = 0;
		// Previous works boolean firstTime = true;
		// Previous works boolean isAllRunOnce = true;
		
		// RetryMonitor is started only after all ComputeRun and ComputeNoBlock are created in Compute class
		// copy m_crunList, m_cnbList to local copy, m_localCRunList, m_localCNBList
		/* Previous works
		synchronized(m_compute) {
			for (DComputeAsyncRun crun : m_crunList) {
				if (false == m_localCRunList.contains(crun))
					m_localCRunList.add(crun);
			}
			for (DComputeNoBlock cnb : m_cnbList) {
				if (false == m_localCNBList.contains(cnb))
					m_localCNBList.add(cnb);
			}
		}
		*/
		
		while(true) {
			
		fcTotal = 0;
		// Previous works isAllRunOnce = true;
		boolean isDone = true;
		boolean isRetrySubmitted = false;
		
		for (DataAsyncRun_D2 crun : m_crunList /* works m_localCRunList*/) {

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

		//log.debug("fcTotal : {}", fcTotal);
		//log.debug("doRetry() isAllRunOnce : {}", isAllRunOnce);
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
			m_cnbWorkingList.addAll(m_localCNBList);
			firstTime = false;
		}

		cnbWorkingTotal = 0;
		for (DComputeNoBlock cnb : m_cnbWorkingList) {
			if (false == cnb.isFaulty()) {
				cnbWorkingTotal = cnbWorkingTotal + 1;
			}
		}
		
		for (DComputeNoBlock cnb : m_cnbWorkingList) {
			if (false == m_localCNBList.contains(cnb))
				m_localCNBList.add(cnb);
		}

		for (DComputeNoBlock cnb : m_localCNBList) {
			if (m_cnbWorkingList.contains(cnb) && cnb.isFaulty()) {
				m_cnbWorkingList.remove(cnb);
				log.debug("removing faulty cnb");
			}
		}
		*/
		
		//TODO make a faulty csynclist
		/*
		if (fcTotal > cnbWorkingTotal) {
			try {
				List<DComputeNoBlock> cnba = m_meta.getComputeNoBlockManyMayExclude(fcTotal - cnbWorkingTotal, m_cnbWorkingList);
				if(cnba != null) {
					for (DComputeNoBlock cnb : cnba) {
						m_cnbWorkingList.add(0, cnb); // inserts at the beginning
					}
				}
			} catch (Error | RuntimeException e) {
				// continue with existing cnbs
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing cnbs");
				// //throw e;
			} catch (Exception e) {
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing cnbs");
				// //throw new RuntimeException(e);
			} catch (Throwable e) {
				log.debug("doRetry() Client Side Error/Exception occurred");
				log.debug("doRetry() continuing with existing cnbs");
				// //throw e;
			}
			
		}
		*/
		
		m_retryCRunList.clear();
		//Previous works int k = 0;
		ListIterator<DataNoBlock> itr = m_cnbList.listIterator();
		for (DataAsyncRun_D2 crun : m_crunList /* Previous works m_localCRunList*/) {

			synchronized(crun) {
			
			if (crun.getRetriesTillNow() < crun.getMaxRetry() 
					&& true == crun.isError() && true == crun.isDone() 
					&& false == crun.isRetrySubmitted()) {
				log.debug("Inside split for loop");
				log.debug("crun.isError() : {}", crun.isError());
				log.debug("crun.isDone() : {}", crun.isDone());
				log.debug("crun.isRetrySubmitted() : {}", crun.isRetrySubmitted());
				log.debug("crun.getSU() : {}", crun.getSU());
				
				// Previous works if (k >= m_cnbWorkingList.size())
				// Previous works	k = 0;
				// Previous works DComputeNoBlock cnb = m_cnbWorkingList.get(k);
				DataNoBlock cnb = null;
        		boolean check = true;
				long count = 0;
        		while (check) {
	        		if (itr.hasNext()) {
	        			cnb = itr.next();
	        			if (cnb.isFaulty() == false)
	        				check = false;
	        		}
	                else {
	        			itr = m_cnbList.listIterator();
	        			cnb = itr.next();
	        			if (cnb.isFaulty() == false)
	        				check = false;
	        		}
	        		if (false == check)
	        			break;
	        		count++;
	        		if (count >= m_cnbListSize)
	        			throw new DScabiClientException("Unable to find a working CNB", "RAM.DRY.1");
				}
				//log.debug("cnb.toString() : {}", cnb.toString());
				crun.setComputeNB(cnb);
				//Previous works k++;
				m_retryCRunList.add(crun);
				// In Compute class, initialize() method, after thread pool shutdown, m_threadPool is set to null
				if (null == m_compute.getExecutorService()) {
					return 0;
				}

			}
			
			} // End synchronized crun
			
		} // End for
		
		executeRetry();
		
		} // End While
		
		// return 0;
	}
	
	public void run() {
		try {
			doRetry();
		}
		catch (Error | RuntimeException e) {
			e.printStackTrace();
			throw e;
	    } catch (Exception e) {
	        e.printStackTrace();
	        throw new RuntimeException(e);
	    } catch (Throwable e) {
	    	e.printStackTrace();
	        throw new RuntimeException(e);
	    }

	}
}
