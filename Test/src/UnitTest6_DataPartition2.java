/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 12-Apr-2016
 * File Name : UnitTest6_DataPartition2.java
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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import com.dilmus.dilshad.scabi.common.DMSeaweedStorageHandler;
import com.dilmus.dilshad.scabi.common.DMStdStorageHandler;
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

/**
 * @author Dilshad Mustafa
 *
 */
public class UnitTest6_DataPartition2 {

	public static void main(String args[]) throws Exception {
		
		// NOTE : change this filename before every run "test_for_CU_13"
		DataContext c = DataContext.dummy();

		DMStdStorageHandler storageHandler = new DMStdStorageHandler();
		// cw DataPartition dp = new DataPartition(c, "mydata2", "mydata2_1", "/home/anees/testdata/bigfile/tutorial/teststorage", "mydata2_1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);
		DataPartition dp = DataPartition.createDataPartition(c, "mydata2", "mydata2_1_app1", "/home/anees/testdata/bigfile/tutorial/teststorage", "mydata2_1_app1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);
	
		// works DMSeaweedStorageHandler storageHandler = new DMSeaweedStorageHandler();
		// works DMSeaweedStorageHandler storageHandler = new DMSeaweedStorageHandler("localhost-8888");
		// works for Seaweed DataPartition dp = new DataPartition(c, "mydata2", "mydata2_1", "teststorage", "mydata2_1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);
		// for Seaweed DataPartition dp = DataPartition.createDataPartition(c, "mydata2", "mydata2_1_app1", "teststorage", "mydata2_1_app1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);

		exportFileTest(dp);
		importFileTest(dp);
		dp.close();
		dp.operationsSuccess();
		dp.deletePartition();
		storageHandler.close();
		
		System.out.println("done");
	}
	
	public static void exportFileTest(DataPartition dp) throws Exception {
		
		dp.appendField("word", "drive4");
		dp.appendField("wordtype", "[verb, present]");
		dp.appendField("count", 5);
		dp.appendRow();

		dp.appendField("word", "get");
		dp.appendField("wordtype", "[verb, past]");
		dp.appendField("count", 6);
		dp.appendRow();
		
		dp.appendField("word", "Hulk");
		dp.appendField("wordtype", "[noun, NA]");
		dp.appendField("count", 7);
		dp.appendRow();

		dp.flushFiles();
		
		dp.exportToFile("/home/anees/testdata/bigfile/dpexported/myfirstexport.txt");
		
		
	}

	public static void importFileTest(DataPartition dp) throws Exception {
		
		dp.importFromFile("/home/anees/testdata/bigfile/dpexported/myfirstexport.txt");
		
		dp.begin();
		dp.next();
		System.out.println("Current line : " + dp.getString());
		System.out.println("Last index : " + dp.getLastIndex());

		for (DataElement e : dp) {
			System.out.println("DataElement as String : " + e.getString());
		}
		
		// works dp.removeAll();
		System.out.println(dp.getString(0));
		System.out.println(dp.getString(1));
		System.out.println(dp.getString(2));
		// will throw index out of bounds exception System.out.println(dp.getString(3));
	
	}
}
