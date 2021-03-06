/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 28-Jan-2016
 * File Name : UnitTest3.java
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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DMClassLoader;
import com.dilmus.dilshad.scabi.common.DMComputeTemplate;
import com.dilmus.dilshad.scabi.common.DMJson;
import com.dilmus.dilshad.scabi.common.DMJsonHelper;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DUtil;
import com.dilmus.dilshad.scabi.core.DaoHelper;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.DsonHelper;
import com.dilmus.dilshad.scabi.db.DDB;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;
import com.dilmus.dilshad.scabi.deprecated.DComputable;
import com.dilmus.dilshad.scabi.deprecated.DDBOld;
import com.dilmus.dilshad.scabi.deprecated.DObject;
import com.dilmus.dilshad.scabi.deprecated.DResultSetOld;
import com.dilmus.dilshad.scabi.deprecated.DTableOld;
import com.dilmus.dilshad.scabi.deprecated.Dao2;

import com.dilmus.dilshad.scabi.common.DMUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.EnclosingMethodAttribute;

/**
 * @author Dilshad Mustafa
 *
 */
public class UnitTest3 {

	
	static Logger log;
	
	public static void main(String args[]) throws Exception, DScabiException, IOException, ParseException {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		

		final Logger log = LoggerFactory.getLogger(UnitTest3.class);
		UnitTest3.log = log;
		
		try {
			testassist();
			//testjar();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException | NotFoundException | CannotCompileException e) {
			
			e.printStackTrace();
		}
		
		
		
		/*
		log.debug("First UUID : {}", UUID.randomUUID());
		log.debug("Second UUID : {}", UUID.randomUUID());
		log.debug("3rd UUID : {}", UUID.randomUUID());
		log.debug("4th UUID : {}", UUID.randomUUID());
		log.debug("5th UUID : {}", UUID.randomUUID());
		*/	
			
		//String s = DsonHelper.json("<<<Hello>>> >>>Here<<<There>>>Go<<<");
		//log.debug("s is : {}", s);
		
		// test2();
		//test();
		//populate();
		//populate2();
		//populate3();
		//test3();
		//testgridfs2();
		//testgridfs3();
	}
	
	public static void testjar() throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NotFoundException, CannotCompileException {
		   try {
			   FileInputStream fis = new FileInputStream("/home/anees/self/test.jar");
			   //works FileInputStream fis = new FileInputStream("/home/anees/self/scabi.jar");
			   byte b[] = DUtil.toBytesFromInStreamForJavaFiles(fis);
		        
			   /* Reference how to create temp file
		        String PREFIX = "stream2file";
		        String SUFFIX = ".tmp";

		        File tempFile = File.createTempFile(PREFIX, SUFFIX);
		        // tempFile.deleteOnExit();
		            try {
		            	FileOutputStream out = new FileOutputStream(tempFile);
		            	while (jarStream.)
		            	// copy from jarStream to out
		            	
		            } catch (Exception e) {
		            	e.printStackTrace();
		            }
		        */
		        
			   String classNameToRun = "TestNew"; //"test.TestNew";
			   // works String classNameToRun = "ComputeServer";
			   /* works
			   	ByteArrayInputStream bais = new ByteArrayInputStream(b);
			   	JarInputStream jarStream = new JarInputStream(bais);
			   	JarEntry jarEntry;
		        byte buffer[] = new byte[1024*1024];
		        ByteArrayOutputStream baop = new ByteArrayOutputStream();
		        int n = 0;
		        DMClassLoader dmcl = new DMClassLoader();
		        
		        
		            while (true) {
		                jarEntry = jarStream.getNextJarEntry();
		                if (jarEntry == null) {
		                    break;
		                }
		                if(jarEntry.isDirectory() || !jarEntry.getName().endsWith(".class")){
			                continue;
			            }
		                if (jarEntry.getName().endsWith(".class")) {
		    		        String className = jarEntry.getName().substring(0,jarEntry.getName().length()-6);
		    		        className = className.replace('/', '.');
		    		        log.debug("className : {}", className);
		    		        baop.reset();
		    		        while( (n = jarStream.read(buffer, 0, buffer.length)) > 0)
		    		        		baop.write(buffer, 0, n);
		    		        byte buffercls[] = baop.toByteArray();
		    		        try {
		    		        dmcl.findClass(className, buffercls);
		    		        } catch (Error | Exception e) {
		    		        	e.printStackTrace();
		    		        }
		    		        if (jarEntry.getName().contains(classNameToRun)) {
		    		        	log.debug("found first matching name, classNameToRun : {}, className : {}", classNameToRun, className);
		    		        	classNameToRun = className;
		    		        }
		                }
		            } 
		            baop.close();
			        */
		            
		      		DComputeUnit cuu = null;
		      		try {
		      			// works Class df = dmcl.loadClass("com.dilmus.dilshad.scabi.cns.ComputeServer");
		      			// works Class<?> df = dmcl.loadClass("test.TestNew");
		      			log.debug("classNameToRun : {}", classNameToRun);
		      			DMClassLoader dm2 = new DMClassLoader();
		      			String s = dm2.loadJarAndSearchClass("/home/anees/self/test.jar", b, classNameToRun);
		      			// works Class<?> df = dmcl.loadClass(classNameToRun);
		      			// Gives error, Uses default / system's class loader Class<?> df = Class.forName(classNameToRun);
		      			log.debug("s : {}", s);
		      			Class<?> df = dm2.loadClass(s);
		      			//Object ob = df.newInstance();
		      			cuu = (DComputeUnit) df.newInstance();
		      			//String[] args = new String[] { "args0" };
		    	  		//log.debug("args.getClass().getCanonicalName() : {}", args.getClass().getCanonicalName());
		    	   		//Method m = df.getMethod("main", new Class<?>[] {args.getClass()});
		    	  		//Method m = df.getMethod("main", args.getClass());
		    	  		//String sa[] = new String[2];
		    	  		//sa[0] = "arg0";
		    	  		//sa[1] = "arg1";
		    	  		log.debug("Going to invoke method");
		    	  		//String result = (String) m.invoke(ob, (Object[])sa);
		    	  		//String result = (String) m.invoke(null, sa);    	  		
		    	  		DComputeContext dson = new DComputeContext("input", "1");
		    	  		String result = cuu.compute(dson);
		    	  		log.debug("result : {}", result);
		    	  		//return "" + result;
		      		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
		      			e.printStackTrace();
		      			
		      		} catch (ClassCastException e) {
		      			e.printStackTrace();
		      		}

		            
		         
	    } catch (Exception e) {
		        e.printStackTrace();
		}
 
	}

	public static void testassist() throws Exception, IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NotFoundException, CannotCompileException {
		
    	DComputeUnit cu = new DComputeUnit() {
			int x = 0;
			
    		public String compute(DComputeContext jsonInput) {
    			newX();
    			x = x + 3;
    			return "compute() from ComputeUnit " + x;			
    		}
    		
    		private int newX() {
    			x = x + 5;
    			return x;
    		}
    	
    	};
    	MyFirstUnit pn = new MyFirstUnit();
    	//Class<? extends DComputeUnit> p = pn.getClass();
    	//Class<? extends ComputeUnit> p = CU.class;
    	Class<? extends DComputeUnit> p = cu.getClass();
    	//Correctly tells compile time error Class<ComputeUnit> p = PrimeNumber.class;
    	// PrimeNumber pn = new PrimeNumber();
    	//Correctly tells compile time error Class<ComputeUnit> p = pn.getClass();
    	String className = p.getName();
    	log.debug("executeClass() className  : {}", className);
  		String classAsPath = className.replace('.', '/') + ".class";
    	log.debug("executeClass() classAsPath  : {}", classAsPath);

  		InputStream in = p.getClassLoader().getResourceAsStream(classAsPath);
  		byte b[] = DMUtil.toBytesFromInStream(in);
  		log.debug("executeClass() b[] s : {}", b.toString());
  		String hexStr = DMUtil.toHexString(b);
  		log.debug("executeClass() Hex string is : {}", hexStr);
  		
  		byte b2[] = DMUtil.toBytesFromHexStr(hexStr);
  		log.debug("executeClass() to bytes  : {}", b2.toString());
  		
  		for (int i = 0; i < b.length; i++) {
  			//log.debug("executeClass() b : {}, b2 : {}", b[i], b2[i]);
  			if (b[i] != b2[i]) {
  				log.debug("executeClass() b and b2 are not same");
  				break;
  			}
  		}
  		log.debug("executeClass() b and b2 are same");

  		//ClassLoader cl = ClassLoader.getSystemClassLoader() ;
  		boolean proceed = false;
  		DComputeUnit cuu = null;
  		try {
	  		Class<?> df = (new DMClassLoader()).findClass(className, b2);
	  		cuu = (DComputeUnit) df.newInstance();
	  		proceed = true;
  		} catch (SecurityException | InstantiationException | IllegalAccessException e) {
  			//e.printStackTrace();
  			proceed = false;
  		} catch (ClassCastException e) {
  			//return e.getMessage();
  		}
  		
  		if (proceed) {
  			log.debug("ComputeUnit cast is working ok for this object");
  			DComputeContext dson = new DComputeContext("input", "1");
	  		String result = cuu.compute(dson);
	  		//return "" + result;
  		} else {
  			log.debug("ComputeUnit cast is not working for this object. So proceeding with Class copy.");
  			InputStream fis = new ByteArrayInputStream(b2);
	
			// works ClassPool pool = ClassPool.getDefault();
			ClassPool pool = new ClassPool(true);
			pool.appendSystemPath();
			//pool.appendClassPath(new LoaderClassPath(_extraLoader));
			pool.insertClassPath(new LoaderClassPath(Thread.currentThread().getContextClassLoader()));
			//pool.importPackage("com.dilmus.dilshad.scabi.client.Dson");
			CtClass cr = pool.makeClass(fis);
	  		cr.setModifiers(cr.getModifiers() | Modifier.PUBLIC);
	  		log.debug("modifiers : {}", cr.getModifiers());
	  		
	  		//CtClass ct = pool.getAndRename("com.dilmus.test.ComputeTemplate", "CT" + System.nanoTime());
	  		CtClass ct = pool.getAndRename(DMComputeTemplate.class.getCanonicalName(), "CT" + System.nanoTime());
	  		
	  		/* works
	  		CtMethod fMethod = cr.getDeclaredMethod("compute");
		    CtMethod gMethod = CtNewMethod.copy(fMethod, ct, null);
		    log.debug("gMethod.getDeclaringClass() : {}", gMethod.getDeclaringClass()); 
		    ca1.addMethod(gMethod);
		    */
	  		
		    CtMethod amethods[] = cr.getDeclaredMethods();
		    for (CtMethod amethod : amethods) {
		    	CtMethod bmethod = CtNewMethod.copy(amethod, ct, null);
			    ct.addMethod(bmethod);
		    }
		    
		    CtField afields[] = cr.getDeclaredFields();
		    for (CtField afield : afields) {
		    	CtField bfield = new CtField(afield, ct);
			    ct.addField(bfield);
		    }
		    
	  		Class<?> df2 = ct.toClass();
	  		Object ob = df2.newInstance();
	  		Method m = df2.getMethod("compute", Dson.class);
	
	  		Dson dson = new Dson("input", "1");
	 		String s = (String) m.invoke(ob, dson);
	
	  		log.debug("s : {}", s);
  		}
	}
	
	public void testdate() throws ParseException {
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        dateFormatGmt.setTimeZone(TimeZone.getTimeZone("ISO"));
		
        SimpleDateFormat dateFormatFromDB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormatFromDB.setTimeZone(TimeZone.getTimeZone("ISO"));

        String datefromDB = "2016-02-17T07:56:24.800Z";
        
		CharSequence cs1 = "T";
		CharSequence cs2 = "Z";
		String s1 = datefromDB.replace(cs1, " ");
		String s2 = s1.replace(cs2, "");

		Date date2 = dateFormatFromDB.parse(s2);
        String uploadDate = dateFormatGmt.format(date2);
        
        log.debug("uploadDate : {}", uploadDate);

		
	}
	
	public static void test() throws DScabiException, IOException {
		//MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("MetaDB");
		
		DDB ddb = new DDB("localhost", "27017", "MetaDB");
		/*
		String map = "function() { for (var key in this) { emit(key, null); } }";
		String reduce = "function(key, s) { return null; }";
		
		String tableName = "ComputeMetaDataTable";
		DBCollection table = db.getCollection(tableName);
		   MapReduceCommand cmd = new MapReduceCommand(table, map, reduce,
				   	     null, MapReduceCommand.OutputType.INLINE, null);
				   	   MapReduceOutput out = table.mapReduce(cmd);
				   	   //String s = out.getOutputCollection().distinct("_id").toString();
				   	//System.out.println("out.getOutputCollection().distinct " + s);
				   	   for (DBObject o : out.results()) {
				   	    System.out.println(o.toString());
				   	 System.out.println("Key name is : " + o.get("_id").toString());
				   	   }			
		*/
		
		//Dao2 dao = new Dao2(ddb);
		//dao.setTableName("ComputeMetaDataTable");
		//dao.fieldNames();
		
		// Insert
		// works
		//String jsonRow = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4577\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		//String jsonCheck = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4577\" }";
		//dao.insertRow(jsonRow, jsonCheck);
		
		// Query
		// works
		/*
		String jsonQuery = "{ \"Status\" : \"Available\" }";
		//String jsonQuery = "{ \"Status\" : \"Inuse\" }";
		String jsonResult = dao.executeQuery(jsonQuery);
		log.debug("jsonResult : {}, result string length : {}", jsonResult, jsonResult.length());
		log.debug("================================================================");

		// works
		DJson djson = new DJson(jsonResult);
		Set<String> st = djson.keySet();
		for (String s : st) {
			log.debug("s : {}", s);
			if (s.equals("Count"))
				continue;
			DJson djsontemp = new DJson(djson.getString(s));
			log.debug("djson.getString(s) : {}", djson.getString(s));
			
			Set<String> sttemp = djsontemp.keySet();
			for (String stemp : sttemp) {
				log.debug("stemp : {}", stemp);
				log.debug("djsontemp.getString(stemp) : {}", djsontemp.getString(stemp));
			}
		}
		log.debug("================================================================");
		*/
		/*
		{ "_id" : { "$oid" : "56a5ea6552aeac08b01f8a6e"} , "ComputeHost" : "localhost" , "ComputePort" : "4568" , "RegisteredDate" : { "$date" : "2016-01-25T09:27:01.031Z"} , "Status" : "Available"}
		{ "_id" : { "$oid" : "56a72c1552aeac0b109f37b4"} , "ComputeHost" : "localhost2" , "ComputePort" : "4569" , "RegisteredDate" : { "$date" : "2016-01-26T08:19:33.133Z"} , "Status" : "Available"}
		{ "_id" : { "$oid" : "56a7a5a352aeac08ebe4a626"} , "ComputeHost" : "localhost3" , "ComputePort" : "4570" , "RegisteredDate" : { "$date" : "2016-01-26T16:58:11.808Z"} , "Status" : "Available"}
		{ "_id" : { "$oid" : "56a7a60152aeac09495a4b85"} , "ComputeHost" : "localhost4" , "ComputePort" : "4571" , "RegisteredDate" : { "$date" : "2016-01-26T16:59:45.355Z"} , "Status" : "Available"}
		{ "_id" : { "$oid" : "56a8533752aeac0a0f981c27"} , "ComputeHost" : "localhost6" , "ComputePort" : "4572" , "RegisteredDate" : { "$date" : "2016-01-27T05:18:47.368Z"} , "Status" : "Available"}
		{ "_id" : { "$oid" : "56aa54dc52aeac17233ac3f1"} , "ComputeHost" : "localhost" , "ComputePort" : "4573" , "RegisteredDate" : "28 Jan 2016 11:18 PM IST" , "Status" : "Available"}		
		*/
		
		// Update
		// works
		//String jsonUpdate = "{ \"Status\" : \"Available\" }";
		//String jsonWhere = "{ \"Status\" : \"Inuse\" }";
		// works
		//String jsonUpdate = "{ \"Status\" : \"Inuse\" }";
		//String jsonWhere = "{ \"Status\" : \"Available\" }";
		// works
		//String jsonUpdate = "{ \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		//String jsonWhere = "{ \"ComputeHost\" : \"localhost\" }";
		//works
		//String jsonUpdate = "{ \"ComputeHost\" : \"localhost6\" }";
		//String jsonWhere = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4573\" }";
		//works
		//String jsonUpdate = "{ \"ComputeHost\" : \"localhost\" }";
		//String jsonWhere = "{ \"ComputeHost\" : \"localhost6\" }";
	
		//dao.executeUpdate(jsonUpdate, jsonWhere);
		//log.debug("================================================================");
		
		// Delete
		// works
		//String jsonWhere2 = "{ \"ComputePort\" : \"4577\" }";
		//dao.executeRemove(jsonWhere2);
		//log.debug("================================================================");
		
    	DTable table = ddb.getTable("ComputeMetaDataTable");
    	// Find and display
    	DDocument searchQuery = new DDocument();
    	searchQuery.put("Status", "Available");

    	DResultSet cursor = table.find(searchQuery);

    	while (cursor.hasNext()) {
    		System.out.println(cursor.next());
    	}


    	// Find and display
    	DDocument searchQuery2 = new DDocument().append("Status", "Inuse");

    	DResultSet cursor2 = table.find(searchQuery2);

    	while (cursor2.hasNext()) {
    		System.out.println(cursor2.next());
    	}

    	//String s = DJsonHelper.json("{ [Status] : [Available] }");
		//System.out.println("json is : " + s);
		
		
		
		
		
		
	}
	
	public static void test2() throws IOException {
		String jsonInput ="{ \"Empty\" : \"1\" }";	
		
		/* fails
        String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\\\"ComputeMetaDataTable\\\");" +
				"String jsonQuery = \\\"{ \\\"Status\\\":\\\"Available\\\" }\\\";" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
		*/
        // works
        String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\\\"ComputeMetaDataTable\\\");" +
				"String jsonQuery = \\\"{ \\\\\\\"Status\\\\\\\":\\\\\\\"Available\\\\\\\" }\\\";" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
        
		// Result { "bshsource" : "DDAO dao = database.createDAO();dao.setTableName(\"ComputeMetaDataTable\");String jsonQuery = \"{ \"Status\":\"Available\" }\";String jsonResult = dao.executeQuery(jsonQuery);return jsonResult;" }
		String jsonBshScript = "{ \"bshsource\" : \"" + action +"\" }";
		DMJson djson1 = new DMJson(jsonBshScript);
		
		System.out.println("djson1 : " + djson1.toString());
		
		DMJson djson3 = new DMJson(djson1.toString());
		Set<String> st = djson3.keySet();
		for (String s : st) {
			
			System.out.println("key : " + s);
			System.out.println("value : " + djson3.getString(s));
		}

      /* fails
		String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\\\"ComputeMetaDataTable\\\");" +
				"String jsonQuery = \\\"{ \\\"Status\\\":\\\"Available\\\" }\\\";" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
       */
        // Result {"bshsource":"DDAO dao = database.createDAO();dao.setTableName(\\\"ComputeMetaDataTable\\\");String jsonQuery = \\\"{ \\\"Status\\\":\\\"Available\\\" }\\\";String jsonResult = dao.executeQuery(jsonQuery);return jsonResult;","jsoninput":"{ \"Empty\" : \"1\" }"}
       
		// works finally
        /* works
		String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\"ComputeMetaDataTable\");" +
				"String jsonQuery = \"{ \\\"Status\\\":\\\"Available\\\" }\";" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
        */

		// works
        /* works
		String action ="DDAO dao = database.createDAO();" +
				"dao.setTableName(\"ComputeMetaDataTable\");" +
				"String jsonQuery = json.json(\"{ <<<Status>>> : <<<Available>>> }\");" +
		   		"String jsonResult = dao.executeQuery(jsonQuery);" +
				"return jsonResult;";
		*/
		// Result {"bshsource":"DDAO dao = database.createDAO();dao.setTableName(\"ComputeMetaDataTable\");String jsonQuery = json.json(\"{ [Status] : [Available] }\");String jsonResult = dao.executeQuery(jsonQuery);return jsonResult;","jsoninput":"{ \"Empty\" : \"1\" }"}
/*
        DJson djson1 = new DJson("bshsource", action);
		DJson djson2 = djson1.add("jsoninput", jsonInput);
		System.out.println("djson2 : " + djson2.toString());
		
		DJson djson3 = new DJson(djson2.toString());
		Set<String> st = djson3.keySet();
		for (String s : st) {
			
			System.out.println("key : " + s);
			System.out.println("value : " + djson3.getString(s));
		}
	*/	

	}

	/*
		Type : MetaThis	Namespace : org.dilmus.scabi.Meta1 Host : host name of MetaDB used by this Meta, Port, UserID, Pwd, DBName : MetaDB

	Type : MetaRemote	Namespace : org.dilmus.scabi.Meta2 Host : Host1, Port, UserID, Pwd, DBName : MetaDB

	Type : AppTable		Namespace : org.dilmus.scabi.AppTable1 Host : Host1, Port, UserID, Pwd, DBName : AppTableDataDB
									DBCollection : <App specific table name>, AppTableMetaData??

	Type : JavaFile		Namespace : org.dilmus.scabi.JavaFile1 Host : Host1, Port, UserID, Pwd, DBName : JavaFileDB
									DBCollection : JavaFileData, JavaFileMetaData

	Type : File		Namespace : org.dilmus.scabi.File1 Host : Host1, Port, UserID, Pwd, DBName : FileDB
										DBCollection : FileData, FileMetaData

	 */	
	
	public static void populate2() throws DScabiException, IOException {
		//MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("MetaDB");
		
		DDB ddb = new DDB("localhost", "27017", "MetaDB");
		// doesn't work ddaohelp.createTable("NamespaceTable");
		//Dao2 dao = new Dao2(ddb);
		//dao.setTableName("NamespaceTable");
		//dao.fieldNames();
		DTable t = ddb.getTable("NamespaceTable");
		// Insert
		// works

		String uuid1 = UUID.randomUUID().toString();
		String uuid2 = UUID.randomUUID().toString();
		String uuid3 = UUID.randomUUID().toString();
		String uuid4 = UUID.randomUUID().toString();
		String uuid5 = UUID.randomUUID().toString();
		
		String jsonRow1 = "{ \"Type\" : \"MetaThis\", \"Namespace\" : \"MyOrg.Meta1\", \"Host\" : \"localhost\", \"Port\" : \"4567\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"MetaServer\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\", \"SystemType\" : \"MetaServer\", \"SystemUUID\" : \"" + uuid1 + "\" }";
		String jsonRow2 = "{ \"Type\" : \"MetaRemote\", \"Namespace\" : \"MyOrg.Meta2\", \"Host\" : \"localhost\", \"Port\" : \"4567\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"MetaServer\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\", \"SystemType\" : \"MetaServer\", \"SystemUUID\" : \"" + uuid2 + "\" }";
		String jsonRow3 = "{ \"Type\" : \"AppTable\", \"Namespace\" : \"MyOrg.MyTables\", \"Host\" : \"localhost\", \"Port\" : \"27017\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"AppTableDB\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\", \"SystemType\" : \"MongoDB\", \"SystemUUID\" : \"" + uuid3 + "\" }";
		String jsonRow4 = "{ \"Type\" : \"JavaFile\", \"Namespace\" : \"MyOrg.MyJavaFiles\", \"Host\" : \"localhost\", \"Port\" : \"27017\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"JavaFileDB\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\", \"SystemType\" : \"MongoDB\", \"SystemUUID\" : \"" + uuid4 + "\" }";
		String jsonRow5 = "{ \"Type\" : \"File\", \"Namespace\" : \"MyOrg.MyFiles\", \"Host\" : \"localhost\", \"Port\" : \"27017\", \"UserID\" : \"test\", \"Pwd\" : \"hello\", \"SystemSpecificName\" : \"FileDB\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\", \"SystemType\" : \"MongoDB\", \"SystemUUID\" : \"" + uuid5 + "\" }";
		
		
		String jsonCheck = "{ \"Namespace\" : \"org.dilmus.scabi.Meta\" }";
		
		t.insertRow(jsonRow1, jsonCheck);
		t.insertRow(jsonRow2, jsonCheck);
		t.insertRow(jsonRow3, jsonCheck);
		t.insertRow(jsonRow4, jsonCheck);
		t.insertRow(jsonRow5, jsonCheck);
			
    	//DTable table = dao.getTable();
    	// Find and display
    	DDocument searchQuery = new DDocument();
    	searchQuery.put("Status", "Available");

    	DResultSet cursor = t.find(searchQuery);

    	while (cursor.hasNext()) {
    		System.out.println(cursor.next());
    	}
		
	}
	
	
	public static void populate() throws DScabiException, IOException {
		//MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("MetaDB");
		DDB ddb = new DDB("localhost", "27017", "MetaDB");
		
		//Dao2 dao = new Dao2(ddb);
		//dao.setTableName("ComputeMetaDataTable");
		//dao.fieldNames();
		DTable t = ddb.getTable("ComputeMetaDataTable");
		// Insert
		// works

		String jsonRow1 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4568\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		String jsonRow2 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4569\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		String jsonRow3 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4570\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		String jsonRow4 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4571\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		String jsonRow5 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4572\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		String jsonRow6 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4573\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
		
		String jsonCheck = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4567\" }";
		
		t.insertRow(jsonRow1, jsonCheck);
		t.insertRow(jsonRow2, jsonCheck);
		t.insertRow(jsonRow3, jsonCheck);
		t.insertRow(jsonRow4, jsonCheck);
		t.insertRow(jsonRow5, jsonCheck);
		t.insertRow(jsonRow6, jsonCheck);
		
    	//DTable table = ddb.getTable("ComputeMetaDataTable");
    	// Find and display
    	DDocument searchQuery = new DDocument();
    	searchQuery.put("Status", "Available");

    	DResultSet cursor = t.find(searchQuery);

    	while (cursor.hasNext()) {
    		System.out.println(cursor.next());
    	}

	}
	
	public static void populate3() throws DScabiException, IOException {
		//MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("MetaDB");
		DDB ddb = new DDB("localhost", "27017", "MetaDB");
		
		//Dao2 dao = new Dao2(ddb);
		//dao.setTableName("ComputeMetaDataTable");
		//dao.fieldNames();
		DTable t = ddb.getTable("ComputeMetaDataTable");
		// Insert
		// works
		
		String jsonCheck = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4567\" }";

		for (int port = 5001; port < 15000; port++) {
			String jsonRow1 = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"" + port + "\", \"ComputeUser\" : \"test\", \"ComputePwd\" : \"hello\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\", \"StatusDate\" : \"28 Jan 2016 11:18 PM IST\" }";
			t.insertRow(jsonRow1, jsonCheck);
		}
    	//DTable table = ddb.getTable("ComputeMetaDataTable");
    	// Find and display
    	DDocument searchQuery = new DDocument();
    	searchQuery.put("Status", "Available");

    	DResultSet cursor = t.find(searchQuery);

    	while (cursor.hasNext()) {
    		System.out.println(cursor.next());
    	}

	}
	
	
	/*
	public static void test3() throws DScabiException, IOException {
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("MetaDB");
		
		
		DDAO dao = new DDAO(db);
		dao.setTableName("ComputeMetaDataTable");
		dao.fieldNames();
		
		// Insert
		//String jsonRow = "{ \"ComputePort\" : \"4574\", \"ComputeHost\" : \"localhost\", \"ComputePwd\" : \"hello\", \"ComputeUser\" : \"test\", \"RegisteredDate\" : \"28 Jan 2016 11:18 PM IST\", \"Status\" : \"Available\" }";
		//String jsonCheck = "{ \"ComputeHost\" : \"localhost\", \"ComputePort\" : \"4574\" }";
		//dao.insertRow(jsonRow, jsonCheck);
		
		// Query
		// works
		String jsonQuery = "{ \"Status\" : \"Available\" }";
		//String jsonQuery = "{ \"Status\" : \"Inuse\" }";
		String jsonResult = dao.executeQuery(jsonQuery);
		log.debug("jsonResult : {}, result string length : {}", jsonResult, jsonResult.length());
		log.debug("================================================================");

		// works
		DJson djson = new DJson(jsonResult);
		Set<String> st = djson.keySet();
		for (String s : st) {
			log.debug("s : {}", s);
			System.out.println("from sysout s : " + s);
			if (s.equals("count"))
				continue;
			DJson djsontemp = new DJson(djson.getString(s));
			log.debug("djson.getString(s) : {}", djson.getString(s));
			
			Set<String> sttemp = djsontemp.keySet();
			for (String stemp : sttemp) {
				log.debug("stemp : {}", stemp);
				System.out.println("from sysout stemp : " + stemp);
				log.debug("djsontemp.getString(stemp) : {}", djsontemp.getString(stemp));
			}
		}
		log.debug("================================================================");
		
		
		// Delete
		// works
		//String jsonWhere = "{ \"ComputePort\" : \"4574\" }";
		//dao.executeRemove(jsonWhere);
		//log.debug("================================================================");
		
    	DBCollection table = db.getCollection("ComputeMetaDataTable");
    	// Find and display
    	BasicDBObject searchQuery = new BasicDBObject();
    	searchQuery.put("Status", "Available");

    	DBCursor cursor = table.find(searchQuery);

    	while (cursor.hasNext()) {
    		System.out.println(cursor.next());
    	}


    	// Find and display
    	BasicDBObject searchQuery2 
    	    = new BasicDBObject().append("Status", "Inuse");

    	DBCursor cursor2 = table.find(searchQuery2);

    	while (cursor2.hasNext()) {
    		System.out.println(cursor2.next());
    	}

    	String s = DJsonHelper.json("{ [Status] : [Available] }");
		System.out.println("json is : " + s);
		
	}
	*/
	
	public static int testgridfs() {

		long time1;
		long time2;
		long time3;
		long time4;
		
		
		
		try {

			MongoClient mongo = new MongoClient("localhost", 27017);
			DB db = mongo.getDB("JFileDB");
			
			// TODO JFileMetaDataTable should be in MetaDB database
			DBCollection collection = db.getCollection("JFileMetaDataTable");

			String newFileName = "com.dilmus.scabi.testdata.in.App.class";

			File jFile = new File("/home/anees/workspace/testdata/in/App.class");

			// create a JFileTable namespace
			GridFS gfsj = new GridFS(db, "JFileTable");

			// get file from local drive
			GridFSInputFile gfsFile = gfsj.createFile(jFile);

			// set a new filename for identify purpose
			gfsFile.setFilename(newFileName);
			gfsFile.setContentType("class"); // jar, zip, war
			// save the image file into mongoDB
			gfsFile.save();
			
			
			// Let's create a new JSON document with some "metadata" information
			BasicDBObject info = new BasicDBObject();
	    	info.put("DBHost", "localhost");
	    	info.put("DBPort", "27017");
	    	info.put("JFileName", newFileName);
	    	info.put("JFileID", gfsFile.getId());
	    	info.put("JFileMD5", gfsFile.getMD5());
	    	collection.insert(info, WriteConcern.ACKNOWLEDGED);
	    	
			// print the result
			DBCursor cursor = gfsj.getFileList();
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}

	    	DBCursor cursor2 = collection.find();

	    	while (cursor2.hasNext()) {
	    		System.out.println(cursor2.next());
	    	}

			
			// get file by it's filename
			GridFSDBFile jForOutput = gfsj.findOne(newFileName);

			// save it into a new image file
			jForOutput.writeTo("/home/anees/workspace/testdata/out/AppOut.class");

			// remove the file from mongoDB
			// gfsj.remove(gfsj.findOne(newFileName));

			System.out.println("Done");
			mongo.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		
		return 0;
	}

    public static void testgridfs2() throws FileNotFoundException, IOException {
        
		long time1;
		long time2;
		long time3;
		long time4;
		String fileName ="makezip3.rar";
    	
		MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("JFileDB");
		MongoDatabase db = mongo.getDatabase("JFileDB");
        GridFSBucket gridFSBucket = GridFSBuckets.create(db);

         // Get the input stream
        time1 = System.currentTimeMillis();
        InputStream streamToUploadFrom = new FileInputStream("/home/anees/workspace/testdata/in/" + fileName);

        // Create some custom options
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(1024*1024)
                .metadata(new Document("type", "class"));

        ObjectId fileId = gridFSBucket.uploadFromStream(fileName, streamToUploadFrom, options);
        streamToUploadFrom.close();
        time2 = System.currentTimeMillis();
     
         time3 = System.currentTimeMillis();
        Date date = new Date();
        
        FileOutputStream streamToDownloadTo = new FileOutputStream("/home/anees/workspace/testdata/out/" + fileName + "_" + date.toString());
        // latest file with same name in DB
        GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(-1);
        //original file with same file name in DB GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(0);
        
        //downloadOptions.
        gridFSBucket.downloadToStreamByName(fileName, streamToDownloadTo, downloadOptions);
        streamToDownloadTo.close();
        time4 = System.currentTimeMillis();
        
        System.out.println("The fileId of the uploaded file is: " + fileId.toHexString());
        System.out.println("Upload time taken : time2 - time1 : " + (time2 - time1));
        System.out.println("Download time taken : time4 - time3 : " + (time4 - time3));

        /*
         Set the revision of the file to retrieve.

		Revision numbers are defined as follows:

	    0 = the original stored file
	    1 = the first revision
	    2 = the second revision
	    etc..
	    -2 = the second most recent revision
	    -1 = the most recent revision

         */
        
        mongo.close();
        
    }

    public static void testgridfs3() throws FileNotFoundException, IOException {

		long time1;
		long time2;
		long time3;
		long time4;
		int n = 0;
		// "/home/anees/workspace/testdata/in/App3.class"
		String fileName = "makezip4.rar";

		MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("JFileDB");
		MongoDatabase db = mongo.getDatabase("JFileDB");
        GridFSBucket gridFSBucket = GridFSBuckets.create(db);

        // Get the input stream
        time1 = System.currentTimeMillis();
        InputStream streamToUploadFrom = new FileInputStream("/home/anees/workspace/testdata/in/" + fileName);

        // Create some custom options
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(1024*1024)
                .metadata(new Document("type", "class"));

        byte data[] = new byte[64*1024*1024];
        GridFSUploadStream uploadStream = gridFSBucket.openUploadStream(fileName, options);

        while ((n = streamToUploadFrom.read(data)) > 0) {
            uploadStream.write(data, 0, n);
        }
        uploadStream.close();
        time2 = System.currentTimeMillis();
 
       
        time3 = System.currentTimeMillis();
        Date date = new Date();
        FileOutputStream streamToDownloadTo = new FileOutputStream("/home/anees/workspace/testdata/out/" + fileName + "_" + date.toString());
        GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(-1);

        GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStreamByName(fileName, downloadOptions);
        long fileLength = downloadStream.getGridFSFile().getLength();
        byte[] bytesToWriteTo = new byte[64*1024*1024];
        
        while ((n = downloadStream.read(bytesToWriteTo)) > 0) {
        	streamToDownloadTo.write(bytesToWriteTo, 0, n);
        }
        downloadStream.close();
        time4 = System.currentTimeMillis();

        System.out.println("The fileId of the uploaded file is: " + uploadStream.getFileId().toHexString());
        System.out.println("Upload time taken : time2 - time1 : " + (time2 - time1));
        System.out.println("Download time taken : time4 - time3 : " + (time4 - time3));

        streamToDownloadTo.close();
        streamToUploadFrom.close();
        mongo.close();
    }

    
    public static void testgridfs4() throws FileNotFoundException, IOException {
        
		MongoClient mongo = new MongoClient("localhost", 27017);
		//DB db = mongo.getDB("JFileDB");
		MongoDatabase db = mongo.getDatabase("JFileDB");
        GridFSBucket gridFSBucket = GridFSBuckets.create(db);

         // Get the input stream
        InputStream streamToUploadFrom = new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));

        // Create some custom options
        GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(1024)
                .metadata(new Document("type", "file"));

        ObjectId fileId = gridFSBucket.uploadFromStream("test", streamToUploadFrom, options);
        streamToUploadFrom.close();
        System.out.println("The fileId of the uploaded file is: " + fileId.toHexString());

  
        // Get some data to write
        byte[] data = "some data to upload into GridFS".getBytes(StandardCharsets.UTF_8);


        GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("sample_data");
        uploadStream.write(data);
        uploadStream.close();
        System.out.println("The fileId of the uploaded file is: " + uploadStream.getFileId().toHexString());

         /*
        gridFSBucket.find().forEach(new Block<GridFSFile>() {
            
            public void apply(final GridFSFile gridFSFile) {
                System.out.println(gridFSFile.getFilename());
            }
        });
		*/
        
        /*
        gridFSBucket.find(eq("metadata.contentType", "image/png")).forEach(
                new Block<GridFSFile>() {
                    
                    public void apply(final GridFSFile gridFSFile) {
                        System.out.println(gridFSFile.getFilename());
                    }
                });
		*/
        
         FileOutputStream streamToDownloadTo = new FileOutputStream("/tmp/test.txt");
        gridFSBucket.downloadToStream(fileId, streamToDownloadTo);
        streamToDownloadTo.close();

         streamToDownloadTo = new FileOutputStream("/tmp/test.txt");
        GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions().revision(0);
        gridFSBucket.downloadToStreamByName("test", streamToDownloadTo, downloadOptions);
        streamToDownloadTo.close();

        GridFSDownloadStream downloadStream = gridFSBucket.openDownloadStream(fileId);
        int fileLength = (int) downloadStream.getGridFSFile().getLength();
        byte[] bytesToWriteTo = new byte[fileLength];
        downloadStream.read(bytesToWriteTo);
        downloadStream.close();

        System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));

  
        downloadStream = gridFSBucket.openDownloadStreamByName("sample_data");
        fileLength = (int) downloadStream.getGridFSFile().getLength();
        bytesToWriteTo = new byte[fileLength];
        downloadStream.read(bytesToWriteTo);
        downloadStream.close();

        System.out.println(new String(bytesToWriteTo, StandardCharsets.UTF_8));

        gridFSBucket.rename(fileId, "test2");


        gridFSBucket.delete(fileId);
        mongo.close();
    }
	
}
