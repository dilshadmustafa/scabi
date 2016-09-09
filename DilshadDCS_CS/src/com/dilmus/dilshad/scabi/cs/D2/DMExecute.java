/**
 * @author Dilshad Mustafa
 * (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 24-Aug-2016
 * File Name : DMExecute.java
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

package com.dilmus.dilshad.scabi.cs.D2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMComputeTemplate;
import com.dilmus.dilshad.scabi.common.DMCounter;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataUnit;

import bsh.EvalError;
import bsh.Interpreter;
import bsh.ParseException;
import bsh.TargetError;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;
import javassist.Modifier;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMExecute {

	private static final Logger log = LoggerFactory.getLogger(DMExecute.class);
	private static final DMCounter M_DMCOUNTER = new DMCounter();

	private static final DMCounter m_gcCounter = new DMCounter();
	
	private static void gc() {
		m_gcCounter.inc();
		if (m_gcCounter.value() >= 2500) {
			System.gc();
			m_gcCounter.set(0);
		}
	}
	
	public static String dataExecuteForDataUnitOperators(String request) {
		gc();
		DMClassLoader dcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		ClassPool pool = null;
		String result = null;
		DMJson dj = null;
		String taskId = null;
		
		log.debug("dataExecuteForDataUnit() request : {}", request);
		try {
			dj = new DMJson(request);
			taskId = dj.getString("TaskId");
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }

			long configCount = dj.getLong("ConfigCount");
		   
		   	for (long i = 1; i <= configCount; i++) {

			String jsonStr = dj.getString("" + i);
		   	DMJson djson = new DMJson(jsonStr);
		   
		   	String hexStr = djson.getString("ClassBytes");
		   	//log.debug("computeExecuteClass() Hex string is : {}", hexStr);
		   	String className = djson.getString("ClassName");
		   	log.debug("dataExecuteForDataUnit() className is : {}", className);
		  	
		   	byte b2[] = DMUtil.toBytesFromHexStr(hexStr);

		   	// Not used ClassLoader cl = ClassLoader.getSystemClassLoader();
	  		boolean proceed = false;
	  		DataUnit cuu = null;
	  		try {
	  			dcl = new DMClassLoader();
	  			Class<?> df = dcl.findClass(className, b2);
	  			
	  			if(false == djson.contains("AddJars")) {
	  				log.debug("dataExecuteForDataUnit() Information only : No additional jars to load. AddJars are not provided.");
	  				Thread.currentThread().setContextClassLoader(dcl);
	  			} else {
	  				log.debug("dataExecuteForDataUnit() AddJars are provided");
	  				loadAddJars(dcl, djson);
	  				
	  				//=====Debugging purpose only==========
	  				//log.debug("computeExecuteClass() Going to load class TestNew");
	  				//Class<?> testNew = dcl.loadClass("test.TestNew");
	  				//log.debug("computeExecuteClass() Loaded class TestNew with name : {}", testNew.getName());
	  				//=====================================
	  				
	  				Thread.currentThread().setContextClassLoader(dcl);

	  			}
	  			
		  		cuu = (DataUnit) df.newInstance();
		  		proceed = true;
	  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
	  			//e.printStackTrace();
	  			proceed = false;
	  		} catch (ClassCastException e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	  			dcl = null;
	  			// Not needed System.gc();
	  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	  			return result;
	  		}
	  		
	  		log.debug("dataExecuteForDataUnit() TotalComputeUnit : {}", dj.getTU());
	  		log.debug("dataExecuteForDataUnit() SplitComputeUnit : {}", dj.getCU());
	  		log.debug("dataExecuteForDataUnit() JsonInput : {}", dj.getString("JsonInput"));
			
	  		/* Previous works
	  		Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
	  		Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	  		Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			*/
	  		
	  		DataContext dson1 = new DataContext("TotalComputeUnit", dj.getString("TotalComputeUnit"));
	  		DataContext dson2 = dson1.add("SplitComputeUnit", dj.getString("SplitComputeUnit"));
	    	dson2.add("JobId", dj.getString("JobId"));
	    	dson2.add("ConfigId", dj.getString("ConfigId"));
	    	dson2.add("TaskId", dj.getString("TaskId"));
	  		DataContext dson3 = dson2.add("JsonInput", dj.getString("JsonInput"));
	  		
	  		if (proceed) {
	  			log.debug("dataExecuteForDataUnit() ComputeUnit cast is working ok for this object");
		  		try { 
		  			cuu.load(dson3);
		  		} catch(Throwable e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dcl = null;
		  			// Not needed System.gc();
			   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
			   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   		return result;
			   	}
		  		log.debug("dataExecuteForDataUnit() loaded");
		  		Thread.currentThread().setContextClassLoader(originalLoader);
		  		dcl = null;
		  		// Not needed System.gc();
		  		/* return in the end
		  		result = DMJson.result("ok from CU : " + dj.getCU());
		  		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  		//return DMJson.ok();
		  		return result;
		  		*/
	  		} else {
	  			log.debug("dataExecuteForDataUnit() ComputeUnit cast is not working for this object. So proceeding with Class copy.");
	  			InputStream fis = new ByteArrayInputStream(b2);
		
				//works ClassPool pool = ClassPool.getDefault();
				pool = new ClassPool(true);
				pool.appendSystemPath();
				// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
				pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
				// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
				CtClass cr = pool.makeClass(fis);
		  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
		  		log.debug("dataExecuteForDataUnit() modifiers : {}", cr.getModifiers());
		  		
		  		// Reference CtClass ct = pool.getAndRename("com.dilmus.test.ComputeTemplate", "CT" + System.nanoTime());
		  		log.debug("dataExecuteForDataUnit() DMComputeTemplate.class.getCanonicalName() : {}", DMComputeTemplate.class.getCanonicalName());
		  		CtClass ct = pool.getAndRename(DMComputeTemplate.class.getCanonicalName(), "CT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
		  		   
			    CtMethod amethods[] = cr.getDeclaredMethods();
			    for (CtMethod amethod : amethods) {
			    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
			    	
			    	// OR
			    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
			    	//bmethod.setBody(amethod, null);
			    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
			    	// OR
			    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
			    	
			    	ct.addMethod(bmethod);
			    }
			    
			    CtField afields[] = cr.getDeclaredFields();
			    for (CtField afield : afields) {
			    	CtField bfield = new CtField(afield, ct);
				    ct.addField(bfield);
			    }
			    
			    // Notes : Anonymous class can not define constructor. So no need to copy constructor
			    
		  		Class<?> df2 = ct.toClass();
		  		Object ob = df2.newInstance();
		  		Method m = df2.getMethod("load", DataContext.class);
		
		  		try {
		  			m.invoke(ob, dson3);
		  		} catch(Throwable e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
			  		cr.detach();
			 		ct.detach();
		  			pool = null;
		  			dcl = null;
		  			// Not needed System.gc();
			   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
			   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   		return result;
			   	}
		 		// Release CtClass from ClassPool
		  		cr.detach();
		 		ct.detach();
		 		pool = null;
		 		dcl = null;
		 		
		  		log.debug("dataExecuteForDataUnit() loaded");
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	  			// Not needed System.gc();
	  			/* return in the end
	  			result = DMJson.result("ok from CU : " + dj.getCU());
	  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
	  			// return DMJson.ok();
		  		return result;
		  		*/
	  		}
	   
		   } // End For
	  		
	   	} catch (Error | RuntimeException e) {
  			Thread.currentThread().setContextClassLoader(originalLoader);
  			pool = null;
  			dcl = null;
  			// Not needed System.gc();
  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
  			return result;
	   	} catch(Exception e) {
  			Thread.currentThread().setContextClassLoader(originalLoader);
	   		pool = null;
  			dcl = null;
  			// Not needed System.gc();
	   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	   		return result;
	   	} catch(Throwable e) {
  			Thread.currentThread().setContextClassLoader(originalLoader);
	   		pool = null;
  			dcl = null;
  			// Not needed System.gc();
	   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	   		return result;
	   	}
   
	   result = DMJson.result("ok from CU : " + dj.getCU());
	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
	   // return DMJson.ok();
	   return result;		
	}
	
	public static String computeExecuteCode(String request) {
		gc();
	    String s = null;
	    DMClassLoader dcl = null;
	    String result = null;
	    DMJson djson = null;
	    String taskId = null;
	    
	    log.debug("executeCode() request : {}", request);
	       try {
	    	   djson = new DMJson(request);
	    	   taskId = djson.getString("TaskId");
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }	    	   
	    	   
	    	   dcl = new DMClassLoader();
	    	   if(false == djson.contains("AddJars")) {
	    		   log.debug("executeCode() Information only : No additional jars to load. AddJars are not provided.");
	    	   } else {
	    		   log.debug("executeCode() AddJars are provided");
	    		   loadAddJars(dcl, djson);
	  				
	    		   //=====Debugging purpose only==========
	    		   //log.debug("executeCode() Going to load class TestNew");
	    		   //Class<?> testNew = dcl.loadClass("test.TestNew");
	    		   //log.debug("executeCode() Loaded class TestNew with name : {}", testNew.getName());
	    		   //=====================================
	    	   }
	    	   
	    	   log.debug("executeCode() TotalComputeUnit : {}", djson.getTU());
	    	   log.debug("executeCode() SplitComputeUnit : {}", djson.getCU());
	    	   log.debug("executeCode() JsonInput : {}", djson.getString("JsonInput"));
		  	   
	    	   /* Previous works
	    	   Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
	    	   Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	    	   Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
	           */
	    	   
	    	   DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
	    	   DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	    	   dson2.add("JobId", djson.getString("JobId"));
	    	   dson2.add("ConfigId", djson.getString("ConfigId"));
	    	   dson2.add("TaskId", djson.getString("TaskId"));
	    	   DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
	    	   
	    	   // Previous works DaoHelper ddaohelp = new DaoHelper("localhost", "27017", "MetaDB");
	    	   
	           Interpreter i = new Interpreter();
	           
	           // Not working i.setClassLoader(Thread.currentThread().getContextClassLoader());
	           // Not working i.setClassLoader(ClassLoader.getSystemClassLoader());
	           i.setClassLoader(dcl);

	           // Previous works i.set("jsonInput", dson3);
	           i.set("context", dson3);
	           i.eval("import com.mongodb.*");
	           i.eval("import com.dilmus.dilshad.scabi.common.*");
	           i.eval("import com.dilmus.dilshad.scabi.core.*");
	           
	           String code = null;
	           code = djson.getString("BshSource");
	           log.debug("executeCode() code : {}", code);
	           String pcode = DMUtil.preprocess(code);
	           log.debug("executeCode() pcode  : {}", pcode);
	           s = (String) i.eval(pcode);

	       }
	       catch ( TargetError e ) {
	    	   //s = e.toString();
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch ( ParseException e ) {
	    	   //s = e.toString();
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch ( EvalError e ) {
	    	   //s = e.toString();
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch (Error | RuntimeException e) {
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch (Exception e) {
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       } catch (Throwable e) {
	    	   dcl = null;
	    	   // Not needed System.gc();
	    	   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }	    	   
	    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	    	   return result;
	       }
	       result = DMJson.result(s);
    	   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }	    	   
    	   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
    	   return result;		
	}
	
	public static String computeExecuteClass(String request) {
		gc();
		DMClassLoader dcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		ClassPool pool = null;
		String result = null;   
		DMJson djson = null;
		String taskId = null;
		
		//log.debug("computeExecuteClass() request : {}", request);
			try {
				djson = new DMJson(request);
				taskId = djson.getString("TaskId");
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }			
				
			   String hexStr = djson.getString("ClassBytes");
			   //log.debug("computeExecuteClass() Hex string is : {}", hexStr);
			   String className = djson.getString("ClassName");
			   log.debug("computeExecuteClass() className is : {}", className);
			  	
			   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);

			   /* Previously used working code
			   dcl = new DClassLoader();
			   Class<?> df = dcl.findClass(className, b2);
			   ComputeUnit cuu = (ComputeUnit) df.newInstance();

			   log.debug("computeExecuteClass() TotalComputeUnit : {}", djson.getTU());
			   log.debug("computeExecuteClass() SplitComputeUnit : {}", djson.getSU());
			   log.debug("computeExecuteClass() JsonInput : {}", djson.getString("JsonInput"));
			   
			   Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
			   Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
			   Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			   
			   String result = cuu.compute(dson3);
			   dcl = null;
			   return DJson.result(result);
			   // return "" + result;
			   // return null; // returns HTTP/1.1 204 No Content
			   */
			   
			   	// Not used ClassLoader cl = ClassLoader.getSystemClassLoader();
		  		boolean proceed = false;
		  		DComputeUnit cuu = null;
		  		try {
		  			dcl = new DMClassLoader();
		  			Class<?> df = dcl.findClass(className, b2);
		  			
		  			if(false == djson.contains("AddJars")) {
		  				log.debug("computeExecuteClass() Information only : No additional jars to load. AddJars are not provided.");
		  				Thread.currentThread().setContextClassLoader(dcl);
		  			} else {
		  				log.debug("computeExecuteClass() AddJars are provided");
		  				loadAddJars(dcl, djson);
		  				
		  				//=====Debugging purpose only==========
		  				//log.debug("computeExecuteClass() Going to load class TestNew");
		  				//Class<?> testNew = dcl.loadClass("test.TestNew");
		  				//log.debug("computeExecuteClass() Loaded class TestNew with name : {}", testNew.getName());
		  				//=====================================
		  				
		  				Thread.currentThread().setContextClassLoader(dcl);

		  			}
		  			
			  		cuu = (DComputeUnit) df.newInstance();
			  		proceed = true;
		  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
		  			//e.printStackTrace();
		  			proceed = false;
		  		} catch (ClassCastException e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dcl = null;
		  			// Not needed System.gc();
		  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		  			return result;
		  		}
		  		
		  		log.debug("computeExecuteClass() TotalComputeUnit : {}", djson.getTU());
		  		log.debug("computeExecuteClass() SplitComputeUnit : {}", djson.getCU());
		  		log.debug("computeExecuteClass() JsonInput : {}", djson.getString("JsonInput"));
				
		  		/* Previous works
		  		Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		  		Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
				*/
		  		
		  		DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		    	dson2.add("JobId", djson.getString("JobId"));
		    	dson2.add("ConfigId", djson.getString("ConfigId"));
		    	dson2.add("TaskId", djson.getString("TaskId"));
		  		DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
		  		
		  		if (proceed) {
		  			log.debug("computeExecuteClass() ComputeUnit cast is working ok for this object");
			  		String res = null;
			  		try {
			  			res = cuu.compute(dson3);
			  		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			  		log.debug("computeExecuteClass() res : {}", res);
			  		Thread.currentThread().setContextClassLoader(originalLoader);
			  		dcl = null;
			  		// Not needed System.gc();
			  		result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
			  		
		  		} else {
		  			log.debug("computeExecuteClass() ComputeUnit cast is not working for this object. So proceeding with Class copy.");
		  			InputStream fis = new ByteArrayInputStream(b2);
			
					//works ClassPool pool = ClassPool.getDefault();
					pool = new ClassPool(true);
					pool.appendSystemPath();
					// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
					pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
					// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
					CtClass cr = pool.makeClass(fis);
			  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
			  		log.debug("computeExecuteClass() modifiers : {}", cr.getModifiers());
			  		
			  		// Reference CtClass ct = pool.getAndRename("com.dilmus.test.ComputeTemplate", "CT" + System.nanoTime());
			  		log.debug("computeExecuteClass() DMComputeTemplate.class.getCanonicalName() : {}", DMComputeTemplate.class.getCanonicalName());
			  		CtClass ct = pool.getAndRename(DMComputeTemplate.class.getCanonicalName(), "CT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
			  		   
				    CtMethod amethods[] = cr.getDeclaredMethods();
				    for (CtMethod amethod : amethods) {
				    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
				    	
				    	// OR
				    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
				    	//bmethod.setBody(amethod, null);
				    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
				    	// OR
				    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
				    	
				    	ct.addMethod(bmethod);
				    }
				    
				    CtField afields[] = cr.getDeclaredFields();
				    for (CtField afield : afields) {
				    	CtField bfield = new CtField(afield, ct);
					    ct.addField(bfield);
				    }
				    
				    // Notes : Anonymous class can not define constructor. So no need to copy constructor
				    
			  		Class<?> df2 = ct.toClass();
			  		Object ob = df2.newInstance();
			  		Method m = df2.getMethod("compute", DComputeContext.class);
			
			 		String res = null;
			 		try {
			 			res = (String) m.invoke(ob, dson3);
			 		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				  		cr.detach();
				 		ct.detach();
			  			pool = null;
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			 		// Release CtClass from ClassPool
			 		cr.detach();
			 		ct.detach();
			 		pool = null;
			 		dcl = null;
			 		
			  		log.debug("computeExecuteClass() res : {}", res);
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			// Not needed System.gc();
		  			result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
		  		}
		   
		   	} catch (Error | RuntimeException e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	  			pool = null;
	  			dcl = null;
	  			// Not needed System.gc();
	  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	  			return result;
		   	} catch(Exception e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
	  			dcl = null;
	  			// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	} catch(Throwable e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
	  			dcl = null;
	  			// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }		
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	}		
	}
	
	public static String computeExecuteFromObject(String request) {
		gc();
		DMClassLoader dcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		ClassPool pool = null;
		String result = null;
		DMJson djson = null;
		String taskId = null;
		
		//log.debug("computeExecuteFromObject() request : {}", request);
		   try {
			   djson = new DMJson(request);
			   taskId = djson.getString("TaskId");
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }
			   
			   String hexStr = djson.getString("ClassBytes");
			   //log.debug("computeExecuteFromObject() Hex string is : {}", hexStr);
			   String className = djson.getString("ClassName");
			   log.debug("computeExecuteFromObject() className is : {}", className);
			  	
			   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
			   log.debug("computeExecuteFromObject() to bytes  : {}", b2.toString());
			   /* for debugging
		  	   for (int i = 0; i < b.length; i++) {
		  			//log.debug("computeExecuteFromObject() b : {}, b2 : {}", b[i], b2[i]);
		  			if (b[i] != b2[i]) {
		  				log.debug("computeExecuteFromObject() b and b2 are not same");
		  				break;
		  			}
		  		}
		  		log.debug("computeExecuteFromObject() b and b2 are same");
			    */
		  		// Not used ClassLoader cl = ClassLoader.getSystemClassLoader() ;
		  		boolean proceed = false;
		  		DComputeUnit cuu = null;
		  		try {
		  			dcl = new DMClassLoader();
		  			Class<?> df = dcl.findClass(className, b2);
		  			
		  			if(false == djson.contains("AddJars")) {
		  				log.debug("computeExecuteFromObject() Information only : No additional jars to load. AddJars are not provided.");
		  				Thread.currentThread().setContextClassLoader(dcl);
		  			} else {
		  				log.debug("computeExecuteFromObject() AddJars are provided");
		  				loadAddJars(dcl, djson);
		  				
		  				//=====Debugging purpose only==========
		  				//log.debug("computeExecuteFromObject() Going to load class TestNew");
		  				//Class<?> testNew = dcl.loadClass("test.TestNew");
		  				//log.debug("computeExecuteFromObject() Loaded class TestNew with name : {}", testNew.getName());
		  				//Class<?> mpcu = dcl.loadClass("MyPrimeCheckUnit");
		  				//log.debug("computeExecuteFromObject() Loaded class with name : {}", mpcu.getName());
		  				//=====================================
		  				
		  				Thread.currentThread().setContextClassLoader(dcl);
		  			}
		  			
			  		cuu = (DComputeUnit) df.newInstance();
			  		proceed = true;
		  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
		  			// e.printStackTrace();
		  			proceed = false;
		  		} catch (ClassCastException e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dcl = null;
		  			// Not needed System.gc();
		  			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		  			return result;
		  		}
		  		
		  		log.debug("computeExecuteFromObject() TotalComputeUnit : {}", djson.getTU());
		  		log.debug("computeExecuteFromObject() SplitComputeUnit : {}", djson.getCU());
		  		log.debug("computeExecuteFromObject() JsonInput : {}", djson.getString("JsonInput"));
				   
		  		/* Previous works
		  		Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		  		Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
		  		*/
		  		
		  		DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
		  		DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
		    	dson2.add("JobId", djson.getString("JobId"));
		    	dson2.add("ConfigId", djson.getString("ConfigId"));
		    	dson2.add("TaskId", djson.getString("TaskId"));
		  		DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
		  		
		  		if (proceed) {
		  			log.debug("computeExecuteFromObject() ComputeUnit cast is working ok for this object");
			  		String res = null; 
			  		try {
			  			res = cuu.compute(dson3);
			  		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			  		log.debug("computeExecuteFromObject() res : {}", res);
			  		Thread.currentThread().setContextClassLoader(originalLoader);
			  		dcl = null;
			  		// Not needed System.gc();
			  		result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
			  		
		  		} else {
		  			log.debug("computeExecuteFromObject() ComputeUnit cast is not working for this object. So proceeding with Class copy.");
		  			InputStream fis = new ByteArrayInputStream(b2);
			
					//works ClassPool pool = ClassPool.getDefault();
					pool = new ClassPool(true);
					pool.appendSystemPath();
					// Reference pool.appendClassPath(new LoaderClassPath(_extraLoader));
					pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
					// Reference pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
					CtClass cr = pool.makeClass(fis);
			  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
			  		log.debug("computeExecuteFromObject() modifiers : {}", cr.getModifiers());
			  		
			  		// Reference CtClass ct = pool.getAndRename("com.dilmus.test.ComputeTemplate", "CT" + System.nanoTime());
			  		log.debug("computeExecuteFromObject() DMComputeTemplate.class.getCanonicalName() : {}", DMComputeTemplate.class.getCanonicalName());
			  		CtClass ct = pool.getAndRename(DMComputeTemplate.class.getCanonicalName(), "CT" + System.nanoTime() + "_" + M_DMCOUNTER.inc());
			  		
			  		// Reference
			  		// CtMethod amethod = cr.getDeclaredMethod("compute");
				    // CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
				    // log.debug("computeExecuteFromObject() bmethod.getDeclaringClass() : {}", bmethod.getDeclaringClass()); 
				    // ct.addMethod(bmethod);
				    
				    CtMethod amethods[] = cr.getDeclaredMethods();
				    for (CtMethod amethod : amethods) {
				    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
				    	
				    	// OR
				    	//CtMethod bmethod = new CtMethod(pool.get(String.class.getCanonicalName()), "compute", new CtClass[] {pool.get(Dson.class.getCanonicalName())}, ct);
				    	//bmethod.setBody(amethod, null);
				    	//bmethod.setModifiers(bmethod.getModifiers() | Modifier.PUBLIC);
				    	// OR
				    	//CtMethod bmethod = CtMethod.make(amethod.getMethodInfo(), ct);
				    	
				    	ct.addMethod(bmethod);
				    }
				    
				    CtField afields[] = cr.getDeclaredFields();
				    for (CtField afield : afields) {
				    	CtField bfield = new CtField(afield, ct);
					    ct.addField(bfield);
				    }
				    
				    // Notes : Anonymous class can not define constructor. So no need to copy constructor
				    
			  		Class<?> df2 = ct.toClass();
			  		if (null == df2)
			  			System.out.println("null == df2");

			  		Object ob = df2.newInstance();
			  		if (null == ob)
			  			System.out.println("null == ob");

			  		Method m = df2.getMethod("compute", DComputeContext.class);
			  		if (null == m)
			  			System.out.println("null == m");

			 		String res = null;
			 		try {
			 			res = (String) m.invoke(ob, dson3);
			 		} catch(Throwable e) {
			  			Thread.currentThread().setContextClassLoader(originalLoader);
				  		//cr.detach();
				 		//ct.detach();
			  			pool = null;
			  			dcl = null;
			  			// Not needed System.gc();
				   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
				   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
						synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   		return result;
				   	}
			 		// Release CtClass from ClassPool
			 		/*
			 		java.lang.NullPointerException
			 		at java.util.Hashtable.put(Hashtable.java:459)
			 		at javassist.ClassPool.cacheCtClass(ClassPool.java:258)
			 		at javassist.CtClass.detach(CtClass.java:1350)
			 		at com.dilmus.dilshad.scabi.cns.ComputeServer.computeExecuteFromObject(ComputeServer.java:411)
			 		*/
			 		//cr.detach();
			 		//ct.detach();
			 		pool = null;
			 		dcl = null;
			 		
			  		log.debug("computeExecuteFromObject() res : {}", res);
			  		Thread.currentThread().setContextClassLoader(originalLoader);
			  		// Not needed System.gc();
			  		result = DMJson.result(res);
		  			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
		  			return result;
			  		// // // return "" + result;
		  		}
		  		// // // return null; // returns HTTP/1.1 204 No Content
			   
		   } catch (Error | RuntimeException e) {
			   //e.printStackTrace();
			   Thread.currentThread().setContextClassLoader(originalLoader);
			   pool = null;
			   dcl = null;
			   // Not needed System.gc();
			   result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
			   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   return result;
		   	} catch(Exception e) {
		   		//e.printStackTrace();
		   		Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
		   		dcl = null;
		   		// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return result;
		   	} catch(Throwable e) {
		   		//e.printStackTrace();
		   		Thread.currentThread().setContextClassLoader(originalLoader);
		   		pool = null;
		   		dcl = null;
		   		// Not needed System.gc();
		   		result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
		   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
		   		return DMJson.error(DMUtil.serverErrMsg(e));
		   	}		
	}
	
	public static String computeExecuteClassNameInJar(String request) {
		gc();
		DMClassLoader dmcl = null;
		ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
		String result = null;
		DMJson djson = null;
		String taskId = null;
		
		//log.debug("computeExecuteClassNameInJar() request : {}", request);
		   try {
			   djson = new DMJson(request);
			   taskId = djson.getString("TaskId");
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_STARTED); }
			   
			   String hexStr = djson.getString("JarBytes");
			   //log.debug("computeExecuteClass() Hex string is : {}", hexStr);
			   String classNameInJar = djson.getString("ClassNameInJar");
			   log.debug("computeExecuteClassNameInJar() classNameInJar is : {}", classNameInJar);
			   String jarFilePath = djson.getString("JarFilePath");
			   log.debug("computeExecuteClassNameInJar() JarFilePath is : {}", jarFilePath);
			  	
			   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
	  		
			   log.debug("computeExecuteClassNameInJar() TotalComputeUnit : {}", djson.getTU());
			   log.debug("computeExecuteClassNameInJar() SplitComputeUnit : {}", djson.getCU());
			   log.debug("computeExecuteClassNameInJar() JsonInput : {}", djson.getString("JsonInput"));

			   /* Previous works
			   Dson dson1 = new Dson("TotalComputeUnit", djson.getString("TotalComputeUnit"));
			   Dson dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
			   Dson dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			   */
			   
			   DComputeContext dson1 = new DComputeContext("TotalComputeUnit", djson.getString("TotalComputeUnit"));
			   DComputeContext dson2 = dson1.add("SplitComputeUnit", djson.getString("SplitComputeUnit"));
	    	   dson2.add("JobId", djson.getString("JobId"));
	    	   dson2.add("ConfigId", djson.getString("ConfigId"));
	    	   dson2.add("TaskId", djson.getString("TaskId"));
			   DComputeContext dson3 = dson2.add("JsonInput", djson.getString("JsonInput"));
			   
			   DComputeUnit cuu = null;
					
			   dmcl = new DMClassLoader();
			   String s = dmcl.loadJarAndSearchClass(jarFilePath, b2, classNameInJar);
			   if (null == s) {
				   log.debug("computeExecuteClassNameInJar() No match found for given class name : {}", classNameInJar);
				   result = DMJson.error("CSR.SYM.EXE", "No match found for given class name : " + classNameInJar);
				   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
				   return result;
			   } else {
				   log.debug("computeExecuteClassNameInJar() found first matching name for given class name : {}, matching class name : {}", classNameInJar, s);
			   }
			   Class<?> df = dmcl.loadClass(s);
	 			
			   if(false == djson.contains("AddJars")) {
				   log.debug("computeExecuteClassNameInJar() Information only : No additional jars to load. AddJars are not provided.");
				   Thread.currentThread().setContextClassLoader(dmcl);
			   } else {
				   log.debug("computeExecuteClassNameInJar() AddJars are provided");
				   DMExecute.loadAddJars(dmcl, djson);
	  				
				   //=====Debugging purpose only==========
				   //log.debug("Going to load class TestNew");
				   //Class<?> testNew = dcl.loadClass("test.TestNew");
				   //log.debug("Loaded class TestNew with name : {}", testNew.getName());
				   //=====================================
				   
	 				Thread.currentThread().setContextClassLoader(dmcl);

			   }
			   
			   cuu = (DComputeUnit) df.newInstance();
			   log.debug("computeExecuteClassNameInJar() Going to invoke method");
			   // Not used Dson dson = new Dson("input", "1");
			   String res = null;
			   try {
				   res = cuu.compute(dson3);
			   } catch(Throwable e) {
		  			Thread.currentThread().setContextClassLoader(originalLoader);
		  			dmcl = null;
		  			// Not needed System.gc();
			   		result = DMJson.error("CSR.SYM.APP", DMUtil.serverErrMsg(e));
			   		synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
					synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
			   		return result;
			   }
			   log.debug("computeExecuteClassNameInJar res : {}", res);
			   
			   Thread.currentThread().setContextClassLoader(originalLoader);
			   dmcl = null;
			   // Not needed System.gc();
			   result = DMJson.result(res);
			   synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
			   synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_COMPLETED); }
			   return result;
	 		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
	 			e.printStackTrace();
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
	  		} catch (ClassCastException e) {
	 			e.printStackTrace();
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
	 		} catch (Error | RuntimeException e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
		   	} catch(Exception e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
		   	} catch(Throwable e) {
	  			Thread.currentThread().setContextClassLoader(originalLoader);
	 			dmcl = null;
	 			// Not needed System.gc();
	 			result = DMJson.error("CSR.SYM.EXE", DMUtil.serverErrMsg(e));
	 			synchronized(ComputeServer_D2.m_taskIdResultMap) { ComputeServer_D2.m_taskIdResultMap.put(taskId, result); }
				synchronized(ComputeServer_D2.m_taskIdStatusMap) { ComputeServer_D2.m_taskIdStatusMap.put(taskId, ComputeServer_D2.S_EXECUTION_ERROR); }
	 			return result;
		   	}		
	}
	
   public static int loadAddJars(DMClassLoader dmcl, DMJson djson) throws IOException, DScabiException {
	   if (false == djson.contains("AddJars"))
		   throw new DScabiException("AddJars key not found in input json", "CSR.LAJ.1");

	   String jsonStrAddJars = djson.getString("AddJars");
	   DMJson djsonAddJars = new DMJson(jsonStrAddJars);
	   Set<String> st = djsonAddJars.keySet();
	   
	   for (String s : st) {
		   log.debug("loadAddJars() loading jar no.{}", s); 
		   String hexStr = djsonAddJars.getString(s);
		   byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
		   dmcl.loadJar(s, b2);
	   }
	   return 0;
	   
   }	
	
	
	
	
	
	
}
