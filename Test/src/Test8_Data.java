/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 21-Jan-2016
 * File Name : Test8_Data.java
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.*;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DFile;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.Dao;
import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataPartition;
import com.dilmus.dilshad.scabi.core.DataElement;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.IOperator;
import com.dilmus.dilshad.scabi.core.compute.DComputeNoBlock;
import com.dilmus.dilshad.scabi.core.computesync_D1.DComputeBlock_D1;
import com.dilmus.dilshad.scabi.core.computesync_D1.DComputeSync_D1;
import com.dilmus.dilshad.scabi.core.data.Data;
import com.dilmus.dilshad.scabi.deprecated.DComputable;
import com.dilmus.dilshad.scabi.deprecated.DObject;
import com.dilmus.dilshad.scabi.deprecated.DTableOld;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;
import com.dilmus.dilshad.scabi.common.DMUtil;

import java.math.*;

import static com.mongodb.client.model.Filters.eq;

/**
 * @author Dilshad Mustafa
 *
 */
public class Test8_Data implements Serializable {
	private static Logger log = null;
	

    public static void main(String[] args) throws Exception {
    	
    	// works
    	// -Dscabi.local.dir="/home/anees/testdata/bigfile/tutorial/testlocal"
    	// -Dscabi.storage.provider="dfs"
    	// -Dscabi.dfs.mount.dir="/home/anees/testdata/bigfile/tutorial/teststorage"
    	
 	   	// -Dscabi.storage.provider="seaweedfs"
 	   	// -Dscabi.seaweedfs.config="localhost-8888"

        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
  		//System.setProperty("org.slf4j.simplelogger.defaultlog", "debug");
    	//System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
  		final Logger log = LoggerFactory.getLogger(Test8_Data.class);
  		Test8_Data.log = log;
    	System.out.println("ScabiClient");
  	
    	// Test suite
    	/*
    	FileUnit fu = new FileUnit();
    	System.out.println(fu.getClass().getGenericSuperclass());
    	Class<?> clsa[] = fu.getClass().getInterfaces();
    	System.out.println("clsa length : " + clsa.length);
    	for (Class<?> cls : clsa) {
    		System.out.println(cls.getCanonicalName());
    	}
    	
    	
       	Class<?> clsa2[] = UnitTest6_Data.class.getInterfaces();
    	System.out.println("clsa2 length : " + clsa2.length);
    	for (Class<?> cls : clsa2) {
    		System.out.println(cls.getCanonicalName());
    	}
    	
      	Type clsa3[] = UnitTest6_Data.class.getGenericInterfaces();
    	System.out.println("clsa3 length : " + clsa3.length);
    	for (Type cls : clsa3) {
    		System.out.println(cls.getTypeName());
    	}
    	
    	System.out.println(Serializable.class.getCanonicalName());
    	
    	System.out.println("Count : " + fu.count(DataContext.dummy()));
    	fu.load(DataContext.dummy());
    	*/
    	
       	DMeta meta = new DMeta("localhost", "5000");
       	
    	Dson dson = new Dson("Partitions", "100");
    	//Dson dson = new Dson("Partitions", "11");
    	HashMap<String, String> out1 = new HashMap<String, String>();
    	Data d = new Data(meta, "mydata", FileUnit.class);  
    	d.input(dson);
    	d.output(out1);
    	//d.maxThreads(12);
    	long time1 = System.currentTimeMillis();
    	long timeTillFinish = 0;
    	IOperator iob = new IOperator() {

			@Override
			public void operate(DataPartition a, DataPartition b, DataContext c) throws Exception {
				
				b.append("Hello from DU " + c.getDU());
				for (DataElement e : a) {
					b.append(e.getInt() + 1);
				}
				return;
			}
    		
    	};
    	//works d.operate("mydata", "newdata", iob);
    	d.operate("mydata", "newdata", (a, b, c) -> { 
    												  b.append("Hello from DU " + c.getDU());
													  for (DataElement e : a) {
														b.append(e.getInt() + 1);
													  }
													  return; 
													 });
    	//IOperator iob2 = (a, b, c) -> { return; };
    	//iob2.operate(null, null, null);
    	// loadJavaFileAsHexStr() className  : Test8_Data$$Lambda$2/1375995437
    	// 2016-10-05 22:06:25:591 +0530 [main] [DEBUG] com.dilmus.dilshad.scabi.core.data.DOperatorConfig_1_1 - loadJavaFileAsHexStr() classAsPath  : Test8_Data$$Lambda$2/1375995437.class
    	
    	
    	d.perform();
    	try {
	    	d.finish();
	    	timeTillFinish = System.currentTimeMillis();
	    	System.out.println("Time taken till finish() : " + (timeTillFinish - time1)); 
	    	long n = d.getNoOfSplits();
	    	for (long i = 1; i <= n; i++) {
		    	DataPartition dp = d.getDataPartition("newdata", i);
		    	System.out.println("");
		    	System.out.print("dp-" + i + " : ");
		    	for (DataElement e : dp) {
		    		System.out.print("[" + e.getString() + "] ");
		    	}
		    	System.out.println("");
		    	System.out.print("dp-" + i + " Pretty Print : " + dp.prettyPrint());
		    	dp.close();
	    	}
	    	System.out.println("");
	    	d.deleteData("mydata");
	    	d.deleteData("newdata");
	    	d.close();
	    	meta.close();
    	} catch (Throwable e) {
    		e.printStackTrace();
    	}
    	long time2 = System.currentTimeMillis();
    	System.out.println("Time taken till finish() : " + (timeTillFinish - time1)); 
    	System.out.println("Time taken : " + (time2 - time1)); 
     	if (out1.isEmpty())
     		System.out.println("out1 is empty");
     	Set<String> st1 = out1.keySet();
     	FileOutputStream fout = new FileOutputStream("outputhash.csv");
     	PrintWriter pwriter = new PrintWriter(fout);
     	for (String s : st1) {
     		System.out.println("out1 for s : " + s + " value : " + out1.get(s));
     		pwriter.println(s + "," + out1.get(s));
     		pwriter.flush();
     	}
     	pwriter.close();
       	// End Test suite
    	
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

        //test2();
        //ScabiClient.testAddJar();
        //ScabiClient.testdao();
        //UnitTest5.testnewddb();
        
    	//DMeta meta = new DMeta("localhost", "5000");
        //meta.validate();
        //DComputeSync c = meta.computeAlloc();
        //log.debug("Compute is {}", c);
        
        //DComputeSync c2 = new DComputeSync(meta);
        //log.debug("Compute is {}", c2);
        
        //DComputeSync c3 = new DComputeSync("{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4568\" }");
        //log.debug("Compute is {}", c3);
        
    	//String result = null;
    	/*
  		c2.addJar("/home/anees/self/test.jar");
  		result = c2.executeClass(CU.class);
 		log.debug("result : {}", result);

        CU cu = new CU();
        c2.addJar("/home/anees/self/test.jar");
  		result = c2.executeObject(cu);
   		log.debug("result : {}", result);

  		c2.addJar("/home/anees/self/test.jar");
  		result = c2.executeClassNameInJar("/home/anees/self/test.jar", "TestNew");
   		log.debug("result : {}", result);

        c2.addJar("/home/anees/self/test.jar");
        result = c2.executeCode(action2);	
 		log.debug("result : {}", result);
   		*/
  		// works
        /*
        String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\"ComputeMetaDataTable\");" +
				"String jsonQuery = \"{ \\\"Status\\\":\\\"Available\\\" }\";" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
         */
        //works
        /*
        String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\"ComputeMetaDataTable\");" +
				"String jsonQuery = json.json(\"{ <<<Status>>> : <<<Available>>> }\");" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
        */
        /* works
        String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(@ComputeMetaDataTable@);" +
				"String jsonQuery = @{ @@Status@@ : @@Available@@ }@;" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
        */
        //works
        
        
        //c.executeCode(action);
        //c.executeClass(CU.class);
        
    	/*
    	ComputeUnit cu = new ComputeUnit() {
			int x = 0;
			
    		public String compute(Dson jsonInput) {
    			newX();
    			x = x + 3;
    			return "compute() from ComputeUnit from CNS " + x;			
    		}
    		
    		private int newX() {
    			x = x + 5;
    			return x;
    		}
    	
    	};
    	c.executeObject(cu);
    	 */
 		//System.exit(0);
    	// works
        //meta.getNamespace("myorg.Meta1");
        //meta.getNamespace("org.dilmus.scabi.Meta3"); // negative test
        
        // works
        //meta.findOneMetaThisNS();
        //meta.findOneMetaRemoteNS();
        //meta.findOneAppTableNS();
        //meta.findOneJavaFileNS();
        //meta.findOneFileNS();

        //works
        //Namespace name = meta.findOneFileNS();
        //log.debug("name details : {} {} {} {} {} {} {}", name.getHost(), name.getPort(), name.getNamespace());
        
        //works
        //Dfile f = new Dfile(meta);
        //f.findOneNamespace();
        //f.getNamespace("org.dilmus.scabi.File1");
        //f.setNamespace("org.dilmus.scabi.File1");
        //Namespace name = f.findOneNamespace();
        //f.setNamespace(name);
        //Date date = new Date();
        //f.put("App9.class", "/home/anees/testdata/in/App9.class");
        //f.get("App9.class", "/home/anees/testdata/out/App9.class" + "_" + date.toString());
        
        //works 
        /*
        f.removeFilesIncompleteMetaData("App8.class", "");
        f.removeFilesIncompleteMetaData("App7.class", "");
        f.removeFilesIncompleteMetaData("App6.class", "");
        f.removeFilesIncompleteMetaData("App5.class", "");
        f.removeFilesIncompleteMetaData("App4.class", "");
        f.removeFilesIncompleteMetaData("App3.class", "");
        f.removeFilesIncompleteMetaData("App2.class", "");
        f.removeFilesIncompleteMetaData("App1.class", "");
        */
        //works
        //f.removeAllFilesIncompleteMetaData();
        
        //works
        //String fileName = "App9.class";
        //String strFileID = "56c5baff52aeac13b741bf0f";
        //log.debug("file name : {}, file id : {}, valid or not : {}", fileName, strFileID, f.isValidMetaData(fileName, strFileID));
        
        //f.close();
        //meta.close();
        //===========================================
        // Java File related
        
        // use gridfs inside to put file
        //meta.uploadJavaFile(namespace or jsonNamespace, file name to put, where to read file "/anees/home/testdata/in/App.class");

        // use gridfs inside to put file
        //meta.uploadJavaFileStream(namespace or jsonNamespace, file name to put, where to read file InputStream);

        // use gridfs inside to get file
        //meta.downloadJavaFile(namespace or jsonNamespace, file name to get, where to write file "/home/anees/testdata/out/App.class");

        // use gridfs inside to get file
        //meta.downloadJavaFileStream(namespace or jsonNamespace, file name to get, where to write file OutputStream);
        
        //===========================================
        // Compute related
        
        // compute.executeJarFile(namespace or jsonjsonNamespace, .jar file name to execute, jsonInput)
        // compute.executeJarFile(namespace or jsonjsonNamespace, .jar file name to execute, .class file inside jar, jsonInput)
        // compute.executeClassFile(namespace or jsonjsonNamespace, .class file name to execute, jsonInput)

        //===========================================
        // App Table related
        
        // meta.createTable(namespace or jsonNamespace, table name)
        // meta.insertRow(namespace or jsonNamespace, jsonInsert, jsonCheck)
        // meta.executeQuery(namespace or jsonNamespace, jsonQuery)
        // meta.executeUpdate(namespace or jsonNamespace, jsonUpdate, jsonWhere)
        
        // decide meta.executeQuery(), etc. or client versions of DAO. which one ?
        // DECISION:- don't use meta.executeQuery(), etc. for DB. use client versions DAO, DAOHelper, Json, JsonHelper.
        // and provide client versions of DAO, DAOHelper, Json, JsonHelper. These form part of API 
        // exposed to client.
        
        //===========================================
        // Async Task
        
        // AsyncTask asy = new AsyncTask(Runnable r);
        
    }

	public static void testdfile() throws IOException, ParseException, DScabiClientException, DScabiException {
		/*
		System.out.println(DMUtil.getNamespaceStr("scabi:MyOrg.MyFiles:input.txt"));
		System.out.println(DMUtil.getResourceName("scabi:MyOrg.MyFiles:input.txt"));
		System.out.println(DMUtil.getResourceName("scabi:MyOrg.MyFiles:a"));
		System.out.println(DMUtil.isNamespaceURLStr("scabi:MyOrg.MyFiles: "));
		//System.out.println(DMUtil.getResourceName("scabi:MyOrg.MyFiles: "));
		System.out.println(DMUtil.isNamespaceURLStr("scabi:MyOrg.MyFiles:"));
		//System.out.println(DMUtil.getResourceName("scabi:MyOrg.MyFiles:"));
		*/
 
   	DMeta meta = new DMeta("localhost", "5000");
		
        //works
        DFile f = new DFile(meta);
        f.setNamespace("MyOrg.Files");
        Date date = new Date();
        //works f.put("App9.class", "/home/anees/testdata/in/App9.class");
        //works f.get("App9.class", "/home/anees/testdata/out/App9.class" + "_" + date.toString());
	
		//works f.copy("scabi:MyOrg.Files:App10.class", "scabi:MyOrg.Files:App9.class");
		// works f.copy("App10.class", "scabi:MyOrg.Files:App9.class");
		
        //works f.put("scabi:MyOrg.Files:App11.class", "/home/anees/testdata/in/App9.class");
        //works f.put("App11.class", "/home/anees/testdata/in/App9.class");
        
		// works f.copy("scabi:MyOrg.Files:App12.class", "scabi:MyOrg.Files:App9.class");
		// works f.copy("App12.class", "scabi:MyOrg.Files:App9.class");
        
        //works f.put("scabi:MyOrg.Files:App13.class", "/home/anees/testdata/in/App9.class");
        // works f.put("App13.class", "/home/anees/testdata/in/App9.class");

        // works f.get("scabi:MyOrg.Files:App10.class", "/home/anees/testdata/out/App10.class" + "_" + date.toString());
        // works f.get("App11.class", "/home/anees/testdata/out/App11.class" + "_" + date.toString());
        
        //FileOutputStream fos = new FileOutputStream("/home/anees/testdata/out/App12.class" + "_" + date.toString());
        //works f.get("scabi:MyOrg.Files:App12.class", fos);
       //fos.close();
        
        //FileOutputStream fos2 = new FileOutputStream("/home/anees/testdata/out/App13.class" + "_" + date.toString());
        //f.get("App13.class", fos2);
        //fos2.close();
        
		//works f.copy("scabi:MyOrg.Files:App14.class", "scabi:MyOrg.Files:App9.class");
		//f.copy("App14.class", "scabi:MyOrg.Files:App9.class");
        
        // works f.put("scabi:MyOrg.Files:App15.class", "/home/anees/testdata/in/App9.class");
        //f.put("App15.class", "/home/anees/testdata/in/App9.class");

        meta.close();
	}
	
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
    	DComputeUnit cu2 = new DComputeUnit() {
    		
    		public String compute(DComputeContext jsonInput) {
    			//System.out.println("compute() Testing 2 in remote. I'm from CU class from CNS");
    			return "Hello from this Compute Unit, CU #" + jsonInput.getCU();
    		}
    	};
    	long time1 = System.currentTimeMillis();
    	//String primeresult = cu2.compute(Dson.dummyDson());
    	//String primeresult = c.executeObject(cu2);
    	DComputeSync_D1 c = new DComputeSync_D1(meta);
    	HashMap<String, String> out1 = new HashMap<String, String>();
    	HashMap<String, String> out2 = new HashMap<String, String>();
    	HashMap<String, String> out3 = new HashMap<String, String>();
    	HashMap<String, String> out4 = new HashMap<String, String>();
    	
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

		meta.close();
	}

	public static void testCodeDMDao() throws IOException, DScabiException, ExecutionException, InterruptedException {
		/*
        String action ="dao = new DMDao(@localhost@, @27017@, @MetaDB@);" +
					 	"dao.setTableName(@ComputeMetaDataTable@);" +
					 	"jsonQuery = @{ @@Status@@ : @@Available@@, @@ComputePort@@ : @@5001@@ }@;" +
					 	"jsonResult = dao.executeQuery(jsonQuery);" +
					 	"return jsonResult;";
		*/
        String action ="dao = new DMDao(@localhost@, @27017@, @MetaDB@);" +
					 	"dao.setTableName(@ComputeMetaDataTable@);" +
					 	"jsonQuery = @{ @@Status@@ : @@Available@@ }@;" +
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
    	DComputeUnit cu2 = new DComputeUnit() {
    		
    		public String compute(DComputeContext jsonInput) {
    			//System.out.println("compute() Testing 2 in remote. I'm from CU class from CNS");
    			return "Hello from this Compute Unit, CU #" + jsonInput.getCU();
    		}
    	};
    	long time1 = System.currentTimeMillis();
    	//String primeresult = cu2.compute(Dson.dummyDson());
    	//String primeresult = c.executeObject(cu2);
    	DComputeSync_D1 c = new DComputeSync_D1(meta);
    	HashMap<String, String> out1 = new HashMap<String, String>();
    	HashMap<String, String> out2 = new HashMap<String, String>();
    	HashMap<String, String> out3 = new HashMap<String, String>();
    	HashMap<String, String> out4 = new HashMap<String, String>();
    	
    	/*
    	c.executeObject(cu2).input(Dson.empty()).split(2).output(out1);
       	c.executeClass(CU.class).input(Dson.empty()).split(5).output(out2);
    	//c.executeClass(cu2.getClass()).input(Dson.empty()).maxSplit(2).output(out).perform();
    	*/
    	c.executeCode(action).input(Dson.empty()).split(1).output(out3);
 		//c.executeClassNameInJar("/home/anees/self/test.jar", "TestNew").split(3).output(out4);
 	   	c.perform();
    	c.finish();
    	
    	/*
     	c.addJar("/home/anees/self/test.jar");
     	c.executeCode(action2).input(Dson.empty()).split(4).output(out3);
    	c.perform();
     	c.finish();
		*/
    	
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

		meta.close();
	}
	
	public static void testMetaExclude() throws IOException, ParseException, DScabiClientException {
		DMeta meta = new DMeta("localhost", "5000");
		List<DComputeNoBlock> list = new LinkedList<DComputeNoBlock>();
		// DComputeNoBlock cnb = new DComputeNoBlock("{ \"ComputeHost\" : \"anees\", \"ComputePort\" : \"5002\", \"MAXCSTHREADS\" : \"5\" }");
		DComputeNoBlock cnb = new DComputeNoBlock("{ \"ComputeHost\" : \"anees\", \"ComputePort\" : \"5001\", \"MAXCSTHREADS\" : \"5\" }");
		list.add(cnb);
		List<DComputeNoBlock> cnba = meta.getComputeNoBlockManyMayExclude(5, list);
		
		for (DComputeNoBlock cnbeach : cnba) {
			cnbeach.close();
		}
		cnb.close();
		meta.close();

	}
	
	public static void testdao() throws IOException, java.text.ParseException, DScabiClientException, DScabiException {
		
		
		DMeta meta = new DMeta("localhost", "5000");
		Dao dao = new Dao(meta);
		//dao.setNamespace("MyOrg.Meta1");
		//DTable table = dao.getTable("scabi:MyOrg.Meta1:ComputeMetaDataTable");
		
		DTable table = dao.getTable("scabi:MyOrg.MyTables:Table1");
		/*
		String jsonRow1 = "{ \"EmployeeName\" : \"Rajesh Kumar\", \"EmployeeNumber\" : \"3000\" }";
		String jsonRow2 = "{ \"EmployeeName\" : \"Pradeep\", \"EmployeeNumber\" : \"3001\" }";
		String jsonCheck = "{ \"EmployeeName\" : \"Pradeep\", \"EmployeeNumber\" : \"0\" }";
		
		table.insertRow(jsonRow1, jsonCheck);
		table.insertRow(jsonRow2, jsonCheck);
		 */
		String jsonQuery = "{ \"EmployeeNumber\" : \"3000\" }";
		String jsonResult = table.executeQuery(jsonQuery);
		
		System.out.println(jsonResult);

		meta.close();
		
	}
	
	public static void testnewddb() throws IOException, java.text.ParseException, DScabiClientException, DScabiException {

		DDB ddb = new DDB("localhost", "27017", "AppTableDB");
		DTable t = ddb.getTable("Table1");
		//works t.fieldNames();
		//works t.fieldNamesUsingFindOne();
		// "EmployeeName"
		// "EmployeeNumber"
		
		//works
		//DDocument d = new DDocument();
		//d.append("EmployeeName", "Tester1").append("EmployeeNumber", "3002");
		//t.insert(d);
		
		//works
		//String jsonRow = "{ \"EmployeeName\" : \"Tester2\", \"EmployeeNumber\" : \"3003\" }";
		//String jsonCheck = "{ \"EmployeeName\" : \"Pradeep\", \"EmployeeNumber\" : \"0\" }";
		//t.insertRow(jsonRow, jsonCheck);
	
		// Negative case - works
		//String jsonRow = "{ \"Employee\" : \"Tester3\", \"EmployeeNumber\" : \"3004\" }";
		//String jsonCheck = "{ \"EmployeeName\" : \"Pradeep\", \"EmployeeNumber\" : \"0\" }";
		//t.insertRow(jsonRow, jsonCheck);
		
		// Negative case - works
		//String jsonRow = "{ \"EmployeeNumber\" : \"3004\" }";
		//String jsonCheck = "{ \"EmployeeName\" : \"Pradeep\", \"EmployeeNumber\" : \"0\" }";
		//t.insertRow(jsonRow, jsonCheck);
	
		//works
		/*
		DDocument d2 = new DDocument();
		d2.put("EmployeeName", "Tester2");
		d2.put("EmployeeNumber", "3003");
   	    DDocument newDocument = new DDocument();
	   	newDocument.put("EmployeeName", "Tester2New"); // Available, Inuse, Hold, Blocked
	   	DDocument updateObj = new DDocument();
	   	updateObj.put("$set", newDocument);
		t.update(d2, updateObj);
		*/
		
		//works
		/*
		String jsonRow2 = "{ \"EmployeeName\" : \"Tester2New2New\", \"EmployeeNumber\" : \"3003\" }";
		String jsonWhere = "{ \"EmployeeName\" : \"Tester2New\" }";
		t.executeUpdate(jsonRow2, jsonWhere);
		 */
		
		//works
		/*
   	    DDocument newDocument = new DDocument();
	   	newDocument.put("EmployeeName", "Tester1New"); // Available, Inuse, Hold, Blocked
	   	DDocument updateObj = new DDocument();
	   	updateObj.put("$set", newDocument);
		t.update(eq("EmployeeName", "Tester1"), updateObj);
		 */
		
		//works t.remove(eq("EmployeeName", "Tester2New2New"));
		
		//works
		/*
		DResultSetNew cursor2 = t.find(eq("EmployeeName", "Pradeep"));
		while (cursor2.hasNext()) {
			log.debug("result : {}", cursor2.next().toString());
		}
		 */
		
		//works
		/*
		String jsonQuery = "{ \"EmployeeName\" : \"Pradeep\" }";
		String result = t.executeQuery(jsonQuery);
		log.debug("result : {}", result);
		*/
		
		//works
		/*
		String jsonQuery2 = "{ \"EmployeeName\" : \"Pradeep\" }";
		DResultSetNew cursor = t.executeQueryCursorResult(jsonQuery2);
		while (cursor.hasNext()) {
			log.debug("result : {}", cursor.next().toString());
		}
		*/
		
		//works
		/*
		String jsonRemove = "{ \"EmployeeName\" : \"Tester1New\" }";
		t.executeRemove(jsonRemove);
		*/
	}
    
    public void test() {
        CloseableHttpClient httpClient;

        // use httpClient (no need to close it explicitly)
    	try {
    		httpClient = HttpClientBuilder.create().build();
    	    // use httpClient (no need to close it explicitly)
    		// specify the host, protocol, and port
    		HttpHost target = new HttpHost("localhost", 5000, "http");
    		
    			// specify the get request
    			// works HttpGet getRequest = new HttpGet("/hello");
    			// /forecastrss?p=80020&u=f
    			/*
    			String myString = "{\"id\":123, \"name\":\"Pankaj Kumar\", \"permanent\":true, \"address\":{ \"street\":\"El Camino Real\"," + 
    	            	"\"city\":\"San Jose\", \"zipcode\":95014 }, \"phoneNumbers\":[9988664422, 1234567890]," +
    					"\"role\":\"Developer\" }";
    			*/
    			//String myString = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4568\" }";
    			String myString = "";
    			// {"id":123, "name":"Pankaj Kumar", "permanent":true, "address":{ "street":"El Camino Real","city":"San Jose", "zipcode":95014 }, "phoneNumbers":[9988664422, 1234567890],"role":"Developer" }
    			
    			// HttpPost postRequest = new HttpPost("/hellopost");
    			// HttpPost postRequest = new HttpPost("/Meta/Compute/Register");
    			HttpPost postRequest = new HttpPost("/Meta/Compute/Alloc");
    			
    			// StringEntity params =new StringEntity("details={\"name\":\"myname\",\"age\":\"20\"} ");
    		    StringEntity params =new StringEntity(myString);
    		    
    		    // works postRequest.addHeader("content-type", "application/x-www-form-urlencoded");
    		    postRequest.addHeader("content-type", "application/json");
    		    postRequest.setEntity(params);
    		            			
    			System.out.println("executing request to " + target);
    	
    			// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
    			HttpResponse httpResponse = httpClient.execute(target, postRequest);
    			HttpEntity entity = httpResponse.getEntity();
    		
    			System.out.println("----------------------------------------");
    			System.out.println(httpResponse.getStatusLine());
    			Header[] headers = httpResponse.getAllHeaders();
    			for (int i = 0; i < headers.length; i++) {
    			System.out.println(headers[i]);
    			}
    		System.out.println("----------------------------------------");
    	
    		if (entity != null) {
    			System.out.println(EntityUtils.toString(entity));
    		}
    	
    
    	
    } catch (IOException e) {

        // handle
    	e.printStackTrace();
    }

    finally {
    // When HttpClient instance is no longer needed,
    // shut down the connection manager to ensure
    // immediate deallocation of all system resources
   
    }   
    	
    }

    public static void test2() {
        CloseableHttpClient httpClient;

        // use httpClient (no need to close it explicitly)
    	try {
    		httpClient = HttpClientBuilder.create().build();
    	    // use httpClient (no need to close it explicitly)
    		// specify the host, protocol, and port
    		HttpHost target = new HttpHost("localhost", 5000, "http");
    		
    			// specify the get request
    			// works HttpGet getRequest = new HttpGet("/hello");
    			// /forecastrss?p=80020&u=f
    			/*
    			String myString = "{\"id\":123, \"name\":\"Pankaj Kumar\", \"permanent\":true, \"address\":{ \"street\":\"El Camino Real\"," + 
    	            	"\"city\":\"San Jose\", \"zipcode\":95014 }, \"phoneNumbers\":[9988664422, 1234567890]," +
    					"\"role\":\"Developer\" }";
    			*/
    			String myString = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4568\" }";
    			//String myString = "";
    			// {"id":123, "name":"Pankaj Kumar", "permanent":true, "address":{ "street":"El Camino Real","city":"San Jose", "zipcode":95014 }, "phoneNumbers":[9988664422, 1234567890],"role":"Developer" }
    			
    			// HttpPost postRequest = new HttpPost("/hellopost");
    			// HttpPost postRequest = new HttpPost("/Meta/Compute/Register");
    			HttpPost postRequest = new HttpPost("/Meta/Compute/Alloc");
    			
    			// StringEntity params =new StringEntity("details={\"name\":\"myname\",\"age\":\"20\"} ");
    		    StringEntity params =new StringEntity(myString);
    		    
    		    // works postRequest.addHeader("content-type", "application/x-www-form-urlencoded");
    		    postRequest.addHeader("Content-Type", "application/json");
    		    postRequest.setEntity(params);
    		            			
    			System.out.println("executing request to " + target);
    	
    			// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
    			HttpResponse httpResponse = httpClient.execute(target, postRequest);
    			HttpEntity entity = httpResponse.getEntity();
    		
    			System.out.println("----------------------------------------");
    			System.out.println(httpResponse.getStatusLine());
    			Header[] headers = httpResponse.getAllHeaders();
    			for (int i = 0; i < headers.length; i++) {
    			System.out.println(headers[i]);
    			}
    		System.out.println("----------------------------------------");
    	
    		if (entity != null) {
    			System.out.println(EntityUtils.toString(entity));
    		}
    	
    
    	
    } catch (IOException e) {

        // handle
    	e.printStackTrace();
    }

    finally {
    // When HttpClient instance is no longer needed,
    // shut down the connection manager to ensure
    // immediate deallocation of all system resources
   
    }   
    	
    }

    
}
