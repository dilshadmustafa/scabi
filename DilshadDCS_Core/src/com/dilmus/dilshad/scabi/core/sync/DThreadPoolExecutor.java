/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 28-Feb-2016
 * File Name : DThreadPoolExecutor.java
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;

/**
 * @author Dilshad Mustafa
 *
 */
public class DThreadPoolExecutor extends ThreadPoolExecutor {

	private DComputeSync m_compute = null;
	// Previous works private HashMap<Future<?>, DComputeSyncRun> m_localFutureCRunMap = null;
	
	public DThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
		      					TimeUnit unit, BlockingQueue<Runnable> workQueue, DComputeSync compute) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		m_compute = compute;
		// Previous works m_localFutureCRunMap = new HashMap<Future<?>, DComputeSyncRun>();
	}
	
	protected void beforeExecute(Thread th, Runnable r) {
		  super.beforeExecute(th, r);
		  
		  /* Previous works
		  synchronized (m_compute) {
			  HashMap<Future<?>, DComputeSyncRun> map = m_compute.getFutureCRunMap();
			  if (map != null) {
				  // Compute class adds the future in its map in its own thread after every m_threadPool.submit(crun)
				  // a copy of this map is made here
				  // So even if Compute class initialize() method clears its m_futureCRunMap, it won't affect here
				  m_localFutureCRunMap.putAll(map);
			  }
		  }
		  */
	}
	
	protected void afterExecute(Runnable r, Throwable t) {
		  super.afterExecute(r, t);
	  
		  if (t == null && r instanceof Future<?>) {
			  Future<?> future = (Future<?>) r;
			  try {
				  if (future.isDone()) {
		            future.get();
		          }
			  } catch (CancellationException ce) {
				  DComputeSyncRun crun = m_compute.getFutureCRunMap(future);
				  // Previous works DComputeSyncRun crun = m_localFutureCRunMap.get(future);
				  if (crun != null) {
					  String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(ce));
					  crun.setExecutionError(errorJsonStr);
				  }
			  } catch (ExecutionException ee) {
				  // Not used t = ee.getCause();
				  DComputeSyncRun crun = m_compute.getFutureCRunMap(future);
				  // Previous works DComputeSyncRun crun = m_localFutureCRunMap.get(future);
				  if (crun != null) {
					  String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(ee));
					  crun.setExecutionError(errorJsonStr);
				  }
			  } catch (InterruptedException ie) {
				  DComputeSyncRun crun = m_compute.getFutureCRunMap(future);
				  // Previous works DComputeSyncRun crun = m_localFutureCRunMap.get(future);
				  if (crun != null) {
					  String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(ie));
					  crun.setExecutionError(errorJsonStr);
				  }
				  Thread.currentThread().interrupt(); // ignore/reset
			  }
		  }
		  if (t != null && r instanceof Future<?>) {
			  //System.out.println(t);
			  Future<?> future = (Future<?>) r;
			  DComputeSyncRun crun = m_compute.getFutureCRunMap(future);
			  // Previous works DComputeSyncRun crun = m_localFutureCRunMap.get(future);
			  if (crun != null) {
				  String errorJsonStr = DMJson.error(DMUtil.clientErrMsg(t));
				  crun.setExecutionError(errorJsonStr);
			  }

		  }
		      
	}
	
	@Override
	public void terminated() {
		super.terminated();
	}
		  
}
