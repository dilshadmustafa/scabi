/**
 * @author Dilshad Mustafa
 * (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 12-Apr-2016
 * File Name : UnitTest11_Seaweed.java
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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.dilmus.dilshad.scabi.common.DMSeaweedStorageHandler;
import com.dilmus.dilshad.scabi.common.DMUtil;
import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataElement;
import com.dilmus.dilshad.scabi.core.DataPartition;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.core.compute.DCompute;
import com.dilmus.dilshad.scabi.deprecated.DFieldGroup;
import com.dilmus.dilshad.scabi.core.IShuffle;

/**
 * @author Dilshad Mustafa
 *
 */
public class UnitTest11_Seaweed {

	public static void main(String[] args) throws Exception {
	       System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
	       System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
	       System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
	       System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
	       System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug"); // works debug warn		
	       System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
	  
		long startTime = System.currentTimeMillis();
		DMSeaweedStorageHandler w = null;
		try {
			// DMSeaweedStorageHandler w = new DMSeaweedStorageHandler();
			// String str = "{ \"1\" : \"{ \\\"Host\\\" : \\\"localhost\\\", \\\"Port\\\" : \\\"8888\\\" }\" }"; 
			String str = "localhost-8888";
			w = new DMSeaweedStorageHandler(str);
			
			/*
			w.deleteIfExists("page-0.dat");
			w.deleteIfExists("page-1.dat");
			w.deleteIfExists("page-2.dat");
			w.deleteIfExists("page-3.dat");
			*/
			System.out.println("start");
			w.copyFromLocal("0678889887687/testmydata/meta_data/page-0.dat", "/home/anees/testdata/bigfile/tutorial/teststorage/mydata3_1/meta_data/page-0.dat");
			w.copyFromLocal("0678889887687/testmydata/index/page-0.dat", "/home/anees/testdata/bigfile/tutorial/teststorage/mydata3_1/index/page-0.dat");
			w.copyFromLocal("0678889887687/testmydata/data/page-0.dat", "/home/anees/testdata/bigfile/tutorial/teststorage/mydata3_1/data/page-0.dat");
		
			w.deleteDirIfExists("0678889887687/testmydata");
			w.close();
			System.out.println("done");
		} catch (Exception e) {
			w.close();
			throw e;
		}
		
		long endTime = System.currentTimeMillis();
		
		System.out.println("Total Time taken : " + (endTime - startTime));
	
	}	
	
}
