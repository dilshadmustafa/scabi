/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 29-Feb-2016
 * File Name : DataRangeRunner_D2.java
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

//import java.io.IOException;
//import java.net.ConnectException;
import java.util.LinkedList;
//import java.util.List;
import java.util.ListIterator;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeoutException;

//import org.apache.http.ConnectionClosedException;
//import org.apache.http.HttpResponse;
//import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;

/**
 * @author Dilshad Mustafa
 *
 */

//Lock order inside transaction : CR

public class DataRangeRunner_D2 implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DataRangeRunner_D2.class);
	private Data m_compute = null;
	// Previous works private List<DComputeAsyncRun> m_localCRunList = null;
	private long m_startCRun = 0;
	private long m_endCRun = 0;
	
	private LinkedList<DataAsyncRun_D2> m_crunList = null;
	private long m_crunListSize = 0;
	
	private DMCounter m_gcCounter = new DMCounter();
	
	private void gc() {
		m_gcCounter.inc();
		if (m_gcCounter.value() >= 2500) {
			System.gc();
			m_gcCounter.set(0);
		}
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
	
	public long getStartCRun() {
		return m_startCRun;
	}
	
	public long getEndCRun() {
		return m_endCRun;
	}
	
	public DataRangeRunner_D2(Data compute, long startCRun, long endCRun) throws DScabiException {
		if (startCRun > endCRun)
			throw new DScabiException("startCRun > endCRun", "RRR.RRR.1");
		m_compute = compute;
		m_startCRun = startCRun;
		m_endCRun = endCRun;
		// Previous works m_localCRunList = new ArrayList<DComputeAsyncRun>();
		synchronized(m_compute) {
			// Previous works List<DComputeAsyncRun> crunList = compute.getCRunList();
			m_crunList = compute.getCRunList();
			m_crunListSize = compute.getCRunListSize();
			// Previous works m_localCRunList.addAll(crunList);
		}
		/* Previous works
		if (startCRun >= m_localCRunList.size())
			throw new DScabiException("startCRun >= m_localCRunList.size()", "RRR.RRR.2");
		if (endCRun >= m_localCRunList.size())
			throw new DScabiException("endCRun >= m_localCRunList.size()", "RRR.RRR.3");
		*/
		if (startCRun >= m_crunListSize)
			throw new DScabiException("startCRun >= m_crunListSize", "RRR.RRR.2");
		if (endCRun >= m_crunListSize)
			throw new DScabiException("endCRun >= m_crunListSize", "RRR.RRR.3");
		
	}

	@Override
	public void run() {

		long time1 = System.currentTimeMillis(); // time1 is from the time of submitting the task
		
		LinkedList<DataAsyncRun_D2> listBlockedCRun = new LinkedList<DataAsyncRun_D2>();

		ListIterator<DataAsyncRun_D2> itr = null;
		try {
			itr = DMUtil.iteratorBefore(m_crunList, m_startCRun);
		} catch (DScabiException e) {
			throw new RuntimeException(e);
		}
		for (long i = m_startCRun; i <= m_endCRun; i++ ) {
			DataAsyncRun_D2 crun = null;
			if (itr.hasNext())
				crun = itr.next();
			else {
				throw new RuntimeException(new DScabiException("No more crun", "DRR.RUN.1"));
			}
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
				crun.submitTask();
				cnb.decCountRequests();
				gc();
			}
			else
				listBlockedCRun.add(crun);

		}
		
		LinkedList<DataAsyncRun_D2> allowedCRunList = new LinkedList<DataAsyncRun_D2>();
		boolean check = true;
		while (check) {
			
			for (DataAsyncRun_D2 crun : listBlockedCRun) {
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
					allowedCRunList.add(crun);
					crun.submitTask();
					cnb.decCountRequests();
					gc();
				} else
					check = false;

			} // End for
			
			for (DataAsyncRun_D2 crun : allowedCRunList) {
				listBlockedCRun.remove(crun);
			} // End for
			allowedCRunList.clear();
			
			if (check)
				break;
			check = true;
			
		} // End while		
		
//===============================================================================================================		
		// isDoneProcessing, retrieveResult part
		
		boolean isFirstTime = true;
		long time2 = 0;
		long typicalTimeTaken = 5000; //1000;
		
		LinkedList<DataAsyncRun_D2> listBlockedCRun2 = new LinkedList<DataAsyncRun_D2>();
		
		ListIterator<DataAsyncRun_D2> itr2 = null;
		try {
			itr2 = DMUtil.iteratorBefore(m_crunList, m_startCRun);
		} catch (DScabiException e) {
			throw new RuntimeException(e);
		}
		for (long i = m_startCRun; i <= m_endCRun; i++ ) {
			DataAsyncRun_D2 crun = null;
			if (itr2.hasNext())
				crun = itr2.next();
			else {
				throw new RuntimeException(new DScabiException("No more crun", "DRR.RUN.1"));
			}
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
				int ret = 0;
				long whichTurn = 0;

				while (0 == ret) {
					synchronized(crun) {
						if (false == crun.isRetrySubmitted() && false == crun.isDone() && false == crun.isError()) {
							ret = crun.submitIsDoneProcessing();
							if (ret != 0)
								break;
						} else
							break;
					}
					log.debug("run() still inside while loop. crun.getSU() : {}, whichTurn : {}", crun.getSU(), whichTurn);
					try {
						if (isFirstTime) {
							long waitTime = timeToWait(whichTurn);
							log.debug("run() Going to sleep for waitTime : {}", waitTime);
							Thread.sleep(waitTime);
							whichTurn++;
						}
						else {
							log.debug("run() Going to sleep for typicalTimeTaken : {}", typicalTimeTaken);
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
						log.debug("run() typicalTimeTaken : {}, crun.getSU() : {}, whichTurn : {}", typicalTimeTaken, crun.getSU(), whichTurn); 
						isFirstTime = false;
					}
					synchronized(crun) {
						if (false == crun.isRetrySubmitted() && false == crun.isDone() && false == crun.isError()) {
							crun.submitRetrieveResult();
						}
					}
				}
				cnb.decCountRequests();
			}
			else
				listBlockedCRun2.add(crun);

		}
		
		LinkedList<DataAsyncRun_D2> allowedCRunList2 = new LinkedList<DataAsyncRun_D2>();
		boolean check2 = true;
		while (check2) {
			
			for (DataAsyncRun_D2 crun : listBlockedCRun2) {
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
					allowedCRunList2.add(crun);
					int ret = 0;
					long whichTurn2 = 0;
					while (0 == ret) {
						synchronized(crun) {
							if (false == crun.isRetrySubmitted() && false == crun.isDone() && false == crun.isError()) {
								ret = crun.submitIsDoneProcessing();
								if (ret != 0)
									break;
							} else
								break;
						}
						log.debug("run() still inside while loop. crun.getSU() : {}, whichTurn2 : {}", crun.getSU(), whichTurn2);
						try {
							if (isFirstTime) {
								long waitTime = timeToWait(whichTurn2);
								log.debug("run() Going to sleep for waitTime : {}", waitTime);
								Thread.sleep(waitTime);
								whichTurn2++;
							}
							else {
								log.debug("run() Going to sleep for typicalTimeTaken : {}", typicalTimeTaken);
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
							log.debug("run() typicalTimeTaken : {}, crun.getSU() : {}, whichTurn2 : {}", typicalTimeTaken, crun.getSU(), whichTurn2); 
							isFirstTime = false;
						}
						synchronized(crun) {
							if (false == crun.isRetrySubmitted() && false == crun.isDone() && false == crun.isError()) {
								crun.submitRetrieveResult();
							}
						}
					}					
					cnb.decCountRequests();
				} else
					check2 = false;

			} // End for
			
			for (DataAsyncRun_D2 crun : allowedCRunList2) {
				listBlockedCRun2.remove(crun);
			} // End for
			allowedCRunList2.clear();
			
			if (check2)
				break;
			check2 = true;
			
		} // End while				
		
	}
	
}
