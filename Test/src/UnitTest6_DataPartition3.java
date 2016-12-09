/**
 * @author Dilshad Mustafa
 * (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 12-Apr-2016
 * File Name : UnitTest6_DataPartition3.java
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
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.dilmus.dilshad.scabi.common.DMSeaweedStorageHandler;
import com.dilmus.dilshad.scabi.common.DMStdStorageHandler;
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
public class UnitTest6_DataPartition3 {

	public static void main(String args[]) throws Exception {
		
		System.out.println("Educated : " + "Educated".hashCode());
		System.out.println("Educated : " + DMUtil.hashString("Educated"));
		
		String s1 = "drive6";
		String s2 = "drive4";
		String s3 = "get2";
		String s4 = "Hulk7";
		/*
		long h1 = s1.hashCode() % 4;
		long h2 = s2.hashCode() % 4;
		long h3 = s3.hashCode() % 4;
		long h4 = s4.hashCode() % 4;
		*/
		long h1 = DMUtil.hashString(s1) % 4;
		long h2 = DMUtil.hashString(s2) % 4;
		long h3 = DMUtil.hashString(s3) % 4;
		long h4 = DMUtil.hashString(s4) % 4;
		
		System.out.println("h1 : " + h1);
		System.out.println("h2 : " + h2);
		System.out.println("h3 : " + h3);
		System.out.println("h4 : " + h4);
		
		// NOTE : change this filename before every run "test_for_CU_13". No need if calling .close()
		DataContext c = DataContext.dummy();
		
		// works DMStdStorageHandler storageHandler = new DMStdStorageHandler();
		
		// it's fixed --> simulate local dir not deleted case --> DataPartition dp = new DataPartition(c, "mydata3", "mydata3_1", "teststorage" /*"/home/anees/testdata/bigfile/tutorial/teststorage"*/, "mydata3_1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);
		//DataPartition dp = new DataPartition(c, "mydata3", "mydata3_1", "teststorage" /*"/home/anees/testdata/bigfile/tutorial/teststorage"*/, "mydata3_1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);
		
		// works DataPartition dp = new DataPartition(c, "mydata3", "mydata3_1", "/home/anees/testdata/bigfile/tutorial/teststorage", "mydata3_1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);		
		
		// works DMSeaweedStorageHandler storageHandler = new DMSeaweedStorageHandler();
		DMSeaweedStorageHandler storageHandler = new DMSeaweedStorageHandler("localhost-8888");
		DataPartition dp = new DataPartition(c, "mydata3", "mydata3_1", "teststorage", "mydata3_1", 64 * 1024 * 1024, "/home/anees/testdata/bigfile/tutorial/testlocal", storageHandler);			

		/*
		// To simulate bigarray.get(long) doesn't read from meta-data page file and so doesn't copy to local meta-data page file
		// and so local meta-data folder is not created
		dp.flushFiles(); // clear cache and remove all page folders data, index, meta-data
		for (DataElement e : dp) {
			System.out.println("DataElement as String : " + e.getString());
		}
		// comment any dp.flushFiles(), dp.close() so that local array dir and page dirs are not deleted
		*/
	
		for (DataElement e : dp) {
			System.out.println("DataElement as String : " + e.getString());
		}
		System.out.println("dp.size() : " + dp.size());
		
		/*
		System.out.println("1- first string : " + dp.getString(0));
		dp.begin();
		System.out.println(dp.next().getString());
		
		System.out.println("2- first string : " + dp.getString());
		*/
	
		exportFileTest(dp);
		importFileTest(dp);
		dp.close();
		storageHandler.close();
		
		System.out.println("done");
	}
	
	public static void exportFileTest(DataPartition dp) throws Exception {

		dp.appendField("word", "drive6"); // tu 4, su 1, new is su 3
		dp.appendField("wordtype", "[verb, present]");
		dp.appendField("count", 5);
		dp.appendRow();
		
		dp.appendField("word", "drive4"); // tu 4, su 3, new is su 1
		dp.appendField("wordtype", "[verb, present]");
		dp.appendField("count", 5);
		dp.appendRow();

		dp.appendField("word", "get2"); // tu 4, su 1, new is su 3
		dp.appendField("wordtype", "[verb, past]");
		dp.appendField("count", 6);
		dp.appendRow();
		
		dp.appendField("word", "Hulk7"); // tu 4, su 4, new is su 4
		dp.appendField("wordtype", "[noun, NA]");
		dp.appendField("count", 7);
		dp.appendRow();
		
		dp.flushFiles();
		
		dp.shuffleBy(new IShuffle() {

			@Override
			public Iterable<String> groupValues(DataElement e, DataContext c) throws IOException {
				// TODO Auto-generated method stub
				List<String> list = new LinkedList<String>();
				list.add(e.getField("word"));
				return list;
			}
		});

		for (DataElement e : dp) {
			System.out.println("DataElement as String : " + e.getString());
		}
		
		System.out.println("dp.size() : " + dp.size());
		// System.exit(0);
		dp.exportToFileForGivenSU("/home/anees/testdata/bigfile/dpexported/myfirstexport.txt", 4, 4);
		// System.exit(0);
		
	}

	public static void importFileTest(DataPartition dp) throws IOException, DScabiException {
		
		dp.importFromFile("/home/anees/testdata/bigfile/dpexported/myfirstexport.txt");
		
		for (DataElement e : dp) {
			System.out.println("DataElement as String : " + e.getString());
		}
		
		System.out.println("dp.size() : " + dp.size());
	}
}
