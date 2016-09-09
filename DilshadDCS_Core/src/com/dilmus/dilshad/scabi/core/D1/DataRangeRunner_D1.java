/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 18-Jul-2016
 * File Name : DataRangeRunner_D1.java
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
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.computesync_D1.DComputeSyncRun_D1;
import com.dilmus.dilshad.scabi.core.data.Data;
import com.dilmus.dilshad.scabi.core.data.DataNoBlock;

/**
 * @author Dilshad Mustafa
 *
 */
public class DataRangeRunner_D1 implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DataRangeRunner_D1.class);
	private Data m_compute = null;
	// Previous works private List<DComputeAsyncRun> m_localCRunList = null;
	private long m_startCRun = 0;
	private long m_endCRun = 0;
	
	private LinkedList<DataAsyncRun_D1> m_crunList = null;
	private long m_crunListSize = 0;
	
	public long getStartCRun() {
		return m_startCRun;
	}
	
	public long getEndCRun() {
		return m_endCRun;
	}
	
	public DataRangeRunner_D1(Data compute, long startCRun, long endCRun) throws DScabiException {
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

		LinkedList<DataAsyncRun_D1> listBlockedCRun = new LinkedList<DataAsyncRun_D1>();

		ListIterator<DataAsyncRun_D1> itr = null;
		try {
			itr = DMUtil.iteratorBefore(m_crunList, m_startCRun);
		} catch (DScabiException e) {
			throw new RuntimeException(e);
		}
		for (long i = m_startCRun; i <= m_endCRun; i++ ) {
			// Previous works DComputeAsyncRun crun = m_localCRunList.get(i);
			DataAsyncRun_D1 crun = null;
			if (itr.hasNext())
				crun = itr.next();
			else {
				throw new RuntimeException(new DScabiException("No more crun", "DRR.RUN.1"));
			}
			DataNoBlock cnb = crun.getComputeNB();
			boolean proceed = false;
			synchronized(cnb) {
				if (cnb.isAllowed()) {
					cnb.incCountRequests();
					proceed = true;
				}
			}
			if (proceed)
				crun.run();
			else
				listBlockedCRun.add(crun);

			/* Previous works
			if (cnb.isAllowed())
				crun.run();
			else
				listBlockedCRun.add(crun);
			*/
		}

		ListIterator<DataAsyncRun_D1> itr2 = null;
		try {
			itr2 = DMUtil.iteratorBefore(m_crunList, m_startCRun);
		} catch (DScabiException e) {
			throw new RuntimeException(e);
		}
		for (long i = m_startCRun; i <= m_endCRun; i++ ) {
			// Previous works DComputeAsyncRun crun = m_localCRunList.get(i);
			DataAsyncRun_D1 crun = null;
			if (itr2.hasNext())
				crun = itr2.next();
			else {
				throw new RuntimeException(new DScabiException("No more crun", "DRR.RUN.2"));
			}
			if (listBlockedCRun.contains(crun))
				continue;
			crun.get();
		} // End for
		
		LinkedList<DataAsyncRun_D1> allowedCRunList = new LinkedList<DataAsyncRun_D1>();
		boolean check = true;
		while (check) {
			
			for (DataAsyncRun_D1 crun : listBlockedCRun) {
				DataNoBlock cnb = crun.getComputeNB();
				boolean proceed = false;
				synchronized(cnb) {
					if (cnb.isAllowed()) {
						cnb.incCountRequests();
						proceed = true;
					}
				}
				if (proceed) {
					allowedCRunList.add(crun);
					crun.run();
				} else
					check = false;
				
				/* Previous works
				if (cnb.isAllowed()) {
					allowedCRunList.add(crun);
					crun.run();
				} else
					check = false;
				*/
			} // End for
			
			for (DataAsyncRun_D1 crun : allowedCRunList) {
				listBlockedCRun.remove(crun);
				crun.get();
			} // End for
			allowedCRunList.clear();
			
			if (check)
				break;
			check = true;
			
		} // End while
		
	}
	
}
