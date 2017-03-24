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
public class Test_checklambdadetails implements Serializable {
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
  		final Logger log = LoggerFactory.getLogger(Test_checklambdadetails.class);
  		Test_checklambdadetails.log = log;
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

    	//IOperator iob2 = (a, b, c) -> { return; };
    	//iob2.operate(null, null, null);
    	// loadJavaFileAsHexStr() className  : Test8_Data$$Lambda$2/1375995437
    	// 2016-10-05 22:06:25:591 +0530 [main] [DEBUG] com.dilmus.dilshad.scabi.core.data.DOperatorConfig_1_1 - loadJavaFileAsHexStr() classAsPath  : Test8_Data$$Lambda$2/1375995437.class
    	
       	// End Test suite
    }
    
}
