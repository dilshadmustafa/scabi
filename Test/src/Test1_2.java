/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 08-Mar-2016
 * File Name : Test1_2.java
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

import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.async.DCompute;

/**
 * @author Dilshad Mustafa
 *
 */
public class Test1_2 {

	   public static void main(String[] args) throws Exception {
	        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
	        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
	        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
	        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
	  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
	  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
	  		//System.setProperty("org.slf4j.simplelogger.defaultlog", "debug");
	    	//System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
	  		final Logger log = LoggerFactory.getLogger(Test1_2.class);

		   /* The examples below use DComputeAsync class which internally uses asynchronous non-blocking network I/O
		    * and can submit very large number of split jobs / Compute Units  
		    */
	    	System.out.println("Test1_2");

	    	DMeta meta = new DMeta("localhost", "5000");
	     	DCompute c = new DCompute(meta);
	     	
	     	// The below example show how to add additional Java libraries, jar files
	     	// and shows how to use the classes inside the Compute Unit
	     	// shows addJar() method
	     	Dson jsonInput = new Dson();
	     	jsonInput.add("NumberToCheck", "993960000099397");

	     	DComputeUnit cu2 = new DComputeUnit() {
	     		public String compute(DComputeContext jsonInput) {
	     			MyPrimeCheckUnit cu = new MyPrimeCheckUnit();
	     			return cu.compute(jsonInput);
	     		}
	     	};
	     	HashMap<String, String> out5 = new HashMap<String, String>();
	     	System.out.println("Submitting cu2 Compute Unit through executeObject() to Cluster for execution");
	     	c.addJar("/home/anees/self/MyPrimeCheckUnit.jar"); // Add Java libraries, jar files like this
	     	c.executeObject(cu2).input(jsonInput).split(1).output(out5);
	     	c.perform();
	     	c.finish();

	        if (out5.isEmpty())
	     		System.out.println("out5 is empty");
	     	Set<String> st5 = out5.keySet();
	     	for (String s : st5) {
	     		System.out.println("out5 for s : " + s + " value : " + out5.get(s));
	     	}
 			     	
	     	// The below example shows executeObject() method to submit a Compute Unit. The Compute Unit will internally submit 
	     	// its own Compute Units / split jobs for execution in the Cluster
	     	DComputeUnit cu3 = new DComputeUnit() {
	     		public String compute(DComputeContext jsonInput) {
	    	    	try {
		     			DMeta meta = new DMeta("localhost", "5000");
		    	     	DCompute c = new DCompute(meta);
		    	     	HashMap<String, String> myout = new HashMap<String, String>();
		    	     	//System.out.println("jsonInput.toString() : " + jsonInput.toString());
		    	     
		    	     	c.executeClass(MyPrimeCheckUnit.class).input(jsonInput.getInput()).split(1).output(myout);
		    	     	c.perform();
		    	     	c.finish();
		    	     	return myout.toString();
	    	    	} catch (Exception e) {
	    	    		return e.toString();
	    	    	}
	     			
	     		}
	     	};
	     	HashMap<String, String> out6 = new HashMap<String, String>();
	     	System.out.println("Submitting cu3 Compute Unit through executeObject() to Cluster for execution");
	     	c.addJar("/home/anees/self/MyPrimeCheckUnit.jar"); // Add Java libraries, jar files like this
	     	c.executeObject(cu3).input(jsonInput).split(1).output(out6);
	     	c.perform();
	     	c.finish();

	        if (out6.isEmpty())
	     		System.out.println("out6 is empty");
	     	Set<String> st6 = out6.keySet();
	     	for (String s : st6) {
	     		System.out.println("out6 for s : " + s + " value : " + out6.get(s));
	     	}
	     	
	     	// The below example shows how to add jar files, java libraries to Compute Units submitted from
	     	// within Compute Unit cu4.
	     	// CUs are run inside Compute Servers. jar file paths provided by User are not available inside Compute Servers
	     	// Use addComputeUnitJars() method to add all the jar files provided to this Compute Unit cu4 by User.
	     	
	     	DComputeUnit cu4 = new DComputeUnit() {
	     		public String compute(DComputeContext jsonInput) {
	    	    	try {
		     			DMeta meta = new DMeta("localhost", "5000");
		    	     	DCompute c = new DCompute(meta);
		    	     	HashMap<String, String> myout = new HashMap<String, String>();
		    	     	//System.out.println("jsonInput.toString() : " + jsonInput.toString());
		    	     	
		    	     	// If the class is under a package, use fully qualified class name new A.B() inside action string
		    	     	// or append "import A.B;" first to the action string
		    	        String action =	"p = new MyPrimeCheckUnit();" +
		    	        				"return p.compute(context);";

		    	     	c.addComputeUnitJars();
		    	     	c.executeCode(action).input(jsonInput.getInput()).split(1).output(myout);
		    	     	c.perform();
		    	     	c.finish();
		    	     	return myout.toString();
	    	    	} catch (Exception e) {
	    	    		return e.toString();
	    	    	}
	     			
	     		}
	     	};
	     	HashMap<String, String> out7 = new HashMap<String, String>();
	     	System.out.println("Submitting cu4 Compute Unit through executeObject() to Cluster for execution");
	     	c.addJar("/home/anees/self/MyPrimeCheckUnit.jar"); // Add Java libraries, jar files like this
	     	c.executeObject(cu4).input(jsonInput).split(1).output(out7);
	     	c.perform();
	     	c.finish();

	        if (out7.isEmpty())
	     		System.out.println("out7 is empty");
	     	Set<String> st7 = out7.keySet();
	     	for (String s : st7) {
	     		System.out.println("out7 for s : " + s + " value : " + out7.get(s));
	     	}

	     	meta.close();
	   }

}
