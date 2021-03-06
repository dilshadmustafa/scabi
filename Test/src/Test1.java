/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 06-Mar-2016
 * File Name : Test1.java
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

import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.compute.DCompute;
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dilshad Mustafa
 *
 */
public class Test1 {

	   public static void main(String[] args) throws Exception {
	        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
	        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
	        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
	        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
	  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
	  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
	  		//System.setProperty("org.slf4j.simplelogger.defaultlog", "debug");
	    	//System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
	  		final Logger log = LoggerFactory.getLogger(Test1.class);
		   
	  		/* The examples below use DComputeAsync class which internally uses asynchronous non-blocking network I/O
		    * and can submit very large number of split jobs / Compute Units
		    */
	    	System.out.println("Test1");

	    	DMeta meta = new DMeta("localhost", "5000");
	    			     	
	     	DCompute c = new DCompute(meta);

	     	// Using executeClass() method to submit Compute Units for execution in the Cluster
	     	Dson jsonInput = new Dson();
	     	jsonInput.add("NumberToCheck", "993960000099397");
	     	// Other numbers to try : 20 digits 12764787846358441471, 30 digits 671998030559713968361666935769

	     	HashMap<String, String> out1 = new HashMap<String, String>();
	     	System.out.println("Submitting Compute Units through executeClass() to Cluster for execution");
	     	c.executeClass(MyPrimeCheckUnit.class).input(jsonInput).split(5).maxRetry(3).output(out1);
	     	c.perform();
	     	c.finish();
	     	
	     	// Using executeObject() method to submit Compute Units through object references and objects of Anonymous class
	     	DComputeUnit cu = new DComputeUnit() {
	     		public String compute(DComputeContext jsonInput) {
	     			return "Hello from this Compute Unit CU #" + jsonInput.getCU();
	     		}
	     	};
	     	HashMap<String, String> out2 = new HashMap<String, String>();
	     	System.out.println("Submitting Compute Units through executeObject() to Cluster for execution");
	     	c.executeObject(cu).input(jsonInput).split(3).output(out2);
	     	c.perform();
	     	c.finish();

	     	// Using execudeCode() method to submit java source code chunks for execution in the Cluster
	     	HashMap<String, String> out3 = new HashMap<String, String>();
	     	System.out.println("Submitting Compute Units through executeCode() to Cluster for execution");
	     	c.addJar("/home/anees/self/MyPrimeCheckUnit.jar"); // Add Java libraries, jar files like this
	     	c.executeCode("import MyPrimeCheckUnit;" +
	     				  "cu = new MyPrimeCheckUnit();" +
	     			      "return cu.compute(context);");
	     	c.input(jsonInput).split(1).output(out3).perform();
	     	c.finish();
	     	
	     	// Using executeJar() method to submit Compute Units for execution in the Cluster
	     	HashMap<String, String> out4 = new HashMap<String, String>();
	     	System.out.println("Submitting Compute Units through executeJar() to Cluster for execution");
	     	c.executeJar("/home/anees/self/MyPrimeCheckUnit.jar", "MyPrimeCheckUnit");
	     	c.input(jsonInput).split(2).output(out4).perform();
	     	c.finish();
		     	
	     	if (out1.isEmpty())
	     		System.out.println("out1 is empty");
	     	Set<String> st1 = out1.keySet();
	     	for (String s : st1) {
	     		System.out.println("out1 for s : " + s + " value : " + out1.get(s));
	     	}
	 	    
	     	if (out2.isEmpty())
	     		System.out.println("out2 is empty");
	     	Set<String> st2 = out2.keySet();
	     	for (String s : st2) {
	     		System.out.println("out2 for s : " + s + " value : " + out2.get(s));
	     	}
	    	
	        if (out3.isEmpty())
	     		System.out.println("out3 is empty");
	     	Set<String> st3 = out3.keySet();
	     	for (String s : st3) {
	     		System.out.println("out3 for s : " + s + " value : " + out3.get(s));
	     	}
	     	
	        if (out4.isEmpty())
	     		System.out.println("out4 is empty");
	     	Set<String> st4 = out4.keySet();
	     	for (String s : st4) {
	     		System.out.println("out4 for s : " + s + " value : " + out4.get(s));
	     	}
	     	
	    	/*
	    	HttpResponse httpResponse = null;
	    	String result = null;
	    	Future<HttpResponse> f = null;

	    	DComputeNoBlock cnb = new DComputeNoBlock(meta);
	     	Dson jsonInput2 = new Dson();
	     	jsonInput2.add("NumberToCheck", "993960000099397");

	    	cnb.setInput(jsonInput2.toString());
	     	f = cnb.executeClass(MyPrimeCheckUnit.class);

	  		httpResponse = DComputeNoBlock.get(f);
	  		result = DComputeNoBlock.getResult(httpResponse);
	  		log.debug("result : {}", result);
			*/
	     	
	     	c.close();
	     	meta.close();
	   }
}

