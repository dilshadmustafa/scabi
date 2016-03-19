/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 29-Feb-2016
 * File Name : DRangeRunner.java
 */

/**
Copyright (c) Dilshad Mustafa 2016. All Rights Reserved.

User and Developer License

1. You can use this Software for both Personal use as well as Commercial use, 
with or without paying fee to Dilshad Mustafa. Please read fully below for the 
terms and conditions. You may use this Software or any of its file contents, 
either partially or fully, only if you comply fully with all the terms and 
conditions of this License. 

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
organization, company, legal entity and/or individual. You should not embed any 
modification of this Software source code and/or its compiled object binary in any form, 
either partially or fully.

6. You should not redistribute this Software, including its source code and/or its 
compiled object binary form, under differently named or renamed software. You should 
not publish this Software, including its source code and/or its compiled object binary 
form, modified or original, under your name or your company name or your product name. 
You should not sell this Software to any party, organization, company, legal entity 
and/or individual.

7. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

8. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

9. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

package com.dilmus.dilshad.scabi.core.async;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
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
import com.dilmus.dilshad.scabi.core.DComputeRun;

/**
 * @author Dilshad Mustafa
 *
 */
public class DRangeRunner implements Runnable {

	private final Logger log = LoggerFactory.getLogger(DRangeRunner.class);
	private DComputeAsync m_compute = null;
	private List<DComputeAsyncRun> m_localCRunList = null;
	private int m_startCRun = 0;
	private int m_endCRun = 0;
	
	public int getStartCRun() {
		return m_startCRun;
	}
	
	public int getEndCRun() {
		return m_endCRun;
	}
	
	public DRangeRunner(DComputeAsync compute, int startCRun, int endCRun) throws DScabiException {
		if (startCRun > endCRun)
			throw new DScabiException("startCRun > endCRun", "RRR.RRR.1");
		m_compute = compute;
		m_startCRun = startCRun;
		m_endCRun = endCRun;
		m_localCRunList = new ArrayList<DComputeAsyncRun>();
		synchronized(m_compute) {
			List<DComputeAsyncRun> crunList = compute.getCRunList();
			m_localCRunList.addAll(crunList);
		}
		if (startCRun >= m_localCRunList.size())
			throw new DScabiException("startCRun >= m_localCRunList.size()", "RRR.RRR.2");
		if (endCRun >= m_localCRunList.size())
			throw new DScabiException("endCRun >= m_localCRunList.size()", "RRR.RRR.3");
		
	}

	@Override
	public void run() {
		String result = null;
		List<DComputeAsyncRun> listBlockedCRun = new ArrayList<DComputeAsyncRun>();
		List<DComputeAsyncRun> copyBlockedList = new ArrayList<DComputeAsyncRun>();

		for (int i = m_startCRun; i <= m_endCRun; i++ ) {
			DComputeAsyncRun crun = m_localCRunList.get(i);
			DComputeNoBlock cnb = crun.getComputeNB();
			if (cnb.isAllowed())
				crun.run();
			else
				listBlockedCRun.add(crun);
		}
				
		HttpResponse httpResponse = null;
		
		for (int i = m_startCRun; i <= m_endCRun; i++ ) {
			DComputeAsyncRun crun = m_localCRunList.get(i);
			if (listBlockedCRun.contains(crun))
				continue;
			crun.get();
		} // End for
		
		List<DComputeAsyncRun> allowedCRunList = new ArrayList<DComputeAsyncRun>();
		boolean check = true;
		while (check) {
			
			for (DComputeAsyncRun crun : listBlockedCRun) {
				DComputeNoBlock cnb = crun.getComputeNB();
				if (cnb.isAllowed()) {
					allowedCRunList.add(crun);
					crun.run();
				} else
					check = false;
			} // End for
			
			for (DComputeAsyncRun crun : allowedCRunList) {
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
