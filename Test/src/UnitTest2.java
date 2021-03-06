/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 29-Feb-2016
 * File Name : UnitTest2.java
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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.http.*;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.compute.DCompute;
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;
import com.dilmus.dilshad.scabi.core.computesync_D1.DComputeBlock_D1;
import com.dilmus.dilshad.scabi.core.computesync_D1.DComputeSync_D1;
import com.dilmus.dilshad.scabi.common.DMUtil;

import java.math.*;
/**
 * @author Dilshad Mustafa
 *
 */
public class UnitTest2 {

	private static Logger log = null;
	
	public static void testAddJar() throws IOException, DScabiException, ExecutionException, InterruptedException {
	       String action ="dao = new Dao(@localhost@, @27017@, @MetaDB@);" +
					 	"dao.setTableName(@ComputeMetaDataTable@);" +
					 	"jsonQuery = @{ @@Status@@ : @@Available@@, @@ComputePort@@ : @@4568@@ }@;" +
					 	"jsonResult = dao.executeQuery(jsonQuery);" +
					 	"return jsonResult;";

	        String action2 ="import test.TestNew;" +
	 					 	"t = new TestNew();" +
	 					  	"dao = new Dao(@localhost@, @27017@, @MetaDB@);" +
	 					 	"dao.setTableName(@ComputeMetaDataTable@);" +
	 					 	"jsonQuery = @{ @@Status@@ : @@Available@@, @@ComputePort@@ : @@4568@@ }@;" +
	 					 	"jsonResult = dao.executeQuery(jsonQuery);" +
	 					 	"return @Result1 : @ + t.compute(null) + @ Result2: @ + jsonResult;";

	    	DMeta meta = new DMeta("localhost", "5000");
	    	long time1 = System.currentTimeMillis();
	     	//String primeresult = cu2.compute(Dson.dummyDson());
	     	//String primeresult = c.executeObject(cu2);
	     	DCompute c = new DCompute(meta);
	     	
	    	DComputeUnit cu2 = new DComputeUnit() {
	    		
	    		public String compute(DComputeContext jsonInput) {
	    			//System.out.println("compute() Testing 2 in remote. I'm from CU class from CNS");
	    			return "Hello from this Compute Unit, CU #" + jsonInput.getCU();
	    		}
	    	};

	     	HashMap<String, String> out1 = new HashMap<String, String>();
	     	HashMap<String, String> out2 = new HashMap<String, String>();
	     	HashMap<String, String> out3 = new HashMap<String, String>();
	     	HashMap<String, String> out4 = new HashMap<String, String>();

	     	
	     	//works c.executeObject(cu2).input(Dson.empty()).maxSplit(5000).output(out1);
	    	//c.executeObject(cu2).input(Dson.empty()).maxSplit(100000).output(out1);
	    	//c.executeObject(cu2).input(Dson.empty()).maxSplit(25).output(out1);
	        //c.executeClass(CU.class).input(Dson.empty()).maxSplit(100).output(out2);
	     	//c.executeClass(cu2.getClass()).input(Dson.empty()).maxSplit(2).output(out).perform();
	     	//c.executeCode(action).input(Dson.empty()).maxSplit(7).output(out3);
	       	//c.executeObject(cu2).input(Dson.empty()).split(15).output(out1);

	    	/*
	       	c.executeObject(cu2).input(Dson.empty()).split(2).output(out1);
	       	c.executeClass(CU.class).input(Dson.empty()).split(5).output(out2);
	    	//c.executeClass(cu2.getClass()).input(Dson.empty()).maxSplit(2).output(out).perform();
	    	c.executeCode(action).input(Dson.empty()).split(7).splitRange(2, 6).output(out3);
	 		c.executeClassNameInJar("/home/anees/self/test.jar", "TestNew").split(3).output(out4);
	    	c.perform();
	     	c.finish();
	     	*/
	     	/*
	     	c.addJar("/home/anees/self/test.jar");
	     	c.executeClass(CU.class).input(Dson.empty()).split(5).output(out1);
	    	c.perform();
	     	c.finish();

	     	c.addJar("/home/anees/self/test.jar");
	       	c.executeObject(cu2).input(Dson.empty()).split(2).output(out2);
	    	c.perform();
	     	c.finish();


	     	c.addJar("/home/anees/self/test.jar");
	     	c.executeCode(action2).input(Dson.empty()).split(4).output(out3);
	    	c.perform();
	     	c.finish();
		*/
	     	c.addJar("/home/anees/self/test.jar");
	     	c.executeJar("/home/anees/self/test.jar", "TestNew").split(3).output(out4);
	    	c.perform();
	     	c.finish();

	 		//try {Thread.currentThread().sleep(20000);}
	 		//catch (Exception e) { }
	     	
	     	if (out1.isEmpty())
	     		System.out.println("out1 is empty");
	     	Set<String> st1 = out1.keySet();
	     	for (String s : st1) {
	     		log.debug("out1 for s : {} value : {}", s, out1.get(s));
	     		//System.out.println("out1 s : " + s + " value : " + out1.get(s));
	     	}
	     	
	     	
	        if (out2.isEmpty())
	     		System.out.println("out2 is empty");
	     	Set<String> st2 = out2.keySet();
	     	for (String s : st2) {
	     		log.debug("out2 for s : {} value : {}", s, out2.get(s));
	     		
	     	}
	    	
	        if (out3.isEmpty())
	     		System.out.println("out3 is empty");
	     	Set<String> st3 = out3.keySet();
	     	for (String s : st3) {
	     		log.debug("out3 for s : {} value : {}", s, out3.get(s));
	     		
	     	}
	     	
	        if (out4.isEmpty())
	     		System.out.println("out4 is empty");
	     	Set<String> st4 = out4.keySet();
	     	for (String s : st4) {
	     		log.debug("out4 for s : {} value : {}", s, out4.get(s));
	     		
	     	}

	     	
	    	long time2 = System.currentTimeMillis();
	    	//log.debug("prime number check result : {}", primeresult);

	    	//log.debug("Last prime number : {}", primeresult);
	    	log.debug("Time taken : {}", time2 - time1); 
			System.out.println("Time taken : " + (time2 - time1));

	         
		
	}
	
	public static void testexclude() throws IOException, ParseException, DScabiClientException {
		DMeta meta = new DMeta("localhost", "5000");
		
		List<DComputeNoBlock> cnba = meta.getComputeNoBlockMany(1);
		List<DComputeNoBlock> cnbexclude = new ArrayList<DComputeNoBlock>();
		cnbexclude.addAll(cnba);
		
		List<DComputeNoBlock> cnba2 = meta.getComputeNoBlockManyMayExclude(1, cnbexclude);
		
		for (DComputeNoBlock cnb2 : cnba2) {
			log.debug("cnb2 : {}", cnb2.toString());
		}
		cnba.get(0).close();
		cnba2.get(0).close();
		meta.close();
	}
	
    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
  		//System.setProperty("org.slf4j.simplelogger.defaultlog", "debug");
    	//System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
  		final Logger log = LoggerFactory.getLogger(UnitTest2.class);
  		UnitTest2.log = log;
    	System.out.println("ScabiClient");
    	
        String action ="dao = new Dao(@localhost@, @27017@, @MetaDB@);" +
				 	"dao.setTableName(@ComputeMetaDataTable@);" +
				 	"jsonQuery = @{ @@Status@@ : @@Available@@, @@ComputePort@@ : @@4568@@ }@;" +
				 	"jsonResult = dao.executeQuery(jsonQuery);" +
				 	"return jsonResult;";

        String action2 ="import test.TestNew;" +
 					 	"t = new TestNew();" +
 					  	"dao = new Dao(@localhost@, @27017@, @MetaDB@);" +
 					 	"dao.setTableName(@ComputeMetaDataTable@);" +
 					 	"jsonQuery = @{ @@Status@@ : @@Available@@, @@ComputePort@@ : @@4568@@ }@;" +
 					 	"jsonResult = dao.executeQuery(jsonQuery);" +
 					 	"return @Result1 : @ + t.compute(null) + @ Result2: @ + jsonResult;";

        //ScabiClient2.testAddJar();
        UnitTest2.testexclude();
        //DMJson j = new DMJson("1", "123");
        //j = j.add("1", "234");
        //System.out.println(j);
        /*
    	DMeta meta = new DMeta("localhost", "5000");
    	
    	DComputeAsync c = new DComputeAsync(meta);
    	DMClassLoader dcl = new DMClassLoader();
    	Thread.currentThread().setContextClassLoader(dcl);
    	c.addComputeUnitJars();
    	*/
    	HttpResponse httpResponse = null;
    	String result = null;
    	Future<HttpResponse> f = null;

  		//ComputeNoBlock cnb = new ComputeNoBlock();
  		//cnb.execute();
    	/*
  		DComputeNoBlock cnb = new DComputeNoBlock(meta);
         
  		cnb.addJar("/home/anees/self/test.jar");
  		f = cnb.executeClass(CU.class);

  		httpResponse = DComputeNoBlock.get(f);
  		result = DComputeNoBlock.getResult(httpResponse);
  		log.debug("result : {}", result);

        CU cu = new CU();
        cnb.addJar("/home/anees/self/test.jar");
  		f = cnb.executeObject(cu);
   		
  		httpResponse = DComputeNoBlock.get(f);
  		result = DComputeNoBlock.getResult(httpResponse);
  		log.debug("result : {}", result);

  		cnb.addJar("/home/anees/self/test.jar");
  		f = cnb.executeClassNameInJar("/home/anees/self/test.jar", "TestNew");
  		
  		httpResponse = DComputeNoBlock.get(f);
  		result = DComputeNoBlock.getResult(httpResponse);
  		log.debug("result : {}", result);
  		
        cnb.addJar("/home/anees/self/test.jar");
        f = cnb.executeCode(action2);	
  		
  		httpResponse = DComputeNoBlock.get(f);
  		result = DComputeNoBlock.getResult(httpResponse);
  		log.debug("result : {}", result);
		*/
        /*
        f = cnb.executeCode(action);
        
  		httpResponse = DComputeNoBlock.get(f);
  		result = DComputeNoBlock.getResult(httpResponse);
  		log.debug("result : {}", result);
  		*/
  		//cnb.close();

    	//DComputeNoBlock cnba[] = meta.getComputeNoBlockMany(1);
    	//log.debug("cnba[0].toString() : {}", cnba[0].toString());
  		//DComputeNoBlock cnb = cnba[0];
         /*
         Future<HttpResponse> future = cnb.executeCode(action);
         HttpResponse httpResponse = DComputeNoBlock.get(future);
         String result = DComputeNoBlock.getResult(httpResponse);
         log.debug("result : {}", result);
         */
 
         
         
         
         //meta.close();
         
    }
    
}
