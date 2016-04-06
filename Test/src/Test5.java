/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * @since 13-Mar-2016
 * File Name : Test5.java
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

7. You should not redistribute this Software and/or any modified works of this 
Software, including its source code and/or its compiled object binary form, under 
differently named or renamed software. You should not publish this Software, including 
its source code and/or its compiled object binary form, modified or original, under 
your name or your company name or your product name. You should not sell this Software 
to any party, organization, company, legal entity and/or individual.

8. You agree fully to the terms and conditions of this License of this software product, 
under same software name and/or if it is renamed in future.

9. This software is created and programmed by Dilshad Mustafa and Dilshad holds the 
copyright for this Software and all its source code. You agree that you will not infringe 
or do any activity that will violate Dilshad's copyright of this software and all its 
source code.

10. The Copyright holder of this Software reserves the right to change the terms 
and conditions of this license without giving prior notice.

11. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHOR OR COPYRIGHT HOLDER
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.

*/

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;

import java.io.IOException;
import java.util.Date;

import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DNamespace;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.Dao;
import com.dilmus.dilshad.scabi.core.Dson;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;

/**
 * @author Dilshad Mustafa
 *
 */
public class Test5 {

	public static void main(String args[]) throws IOException, ParseException, DScabiClientException, DScabiException, java.text.ParseException {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		
  		//System.setProperty("org.slf4j.simplelogger.defaultlog", "debug");
    	//System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
  		final Logger log = LoggerFactory.getLogger(Test5.class);
		System.out.println("Test5");
		DMeta meta = new DMeta("localhost", "5000");
		
		Dson dson = new Dson();
    	dson.add("Namespace", "MyCompany-Tables");
    	dson.add("Type", DNamespace.APPTABLE);
    	dson.add("Host", "localhost");
    	dson.add("Port", "27017");
    	dson.add("UserID", "myuser");
    	dson.add("Pwd", "hello");
    	dson.add("SystemSpecificName", "MyCompanyDB");
    	dson.add("SystemType", "MongoDB");

    	try {
    		if (false == meta.namespaceExists("MyCompany-Tables")) {
		    	System.out.println("Register new namespace");
		    	meta.namespaceRegister(dson);
    		}
		} catch (Exception e) {
			e.printStackTrace();
		}

		Dao dao = new Dao(meta);
		
		// The below examples demonstrate CRUD operations
		
		// Create Table
		try {
			if (false == dao.tableExists("scabi:MyCompany-Tables:Table1")) {
				System.out.println("Create Table");
				DTable t = dao.createTable("scabi:MyCompany-Tables:Table1");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Get existing Table
		DTable table = dao.getTable("scabi:MyCompany-Tables:Table1");
		
		// Insert data
		if (0 == table.count()) {
			try {
				System.out.println("Insert data");
				DDocument d = new DDocument();
				d.append("EmployeeName", "Karthik").append("EmployeeNumber", "3000").append("Age",  40);
				table.insert(d);
				
				d.clear();
				d.append("EmployeeName", "Jayaprakash").append("EmployeeNumber", "3001").append("Age",  35);
				table.insert(d);
				
				d.clear();
				d.append("EmployeeName", "Arun").append("EmployeeNumber", "3002").append("Age",  30);
				table.insert(d);
				
				d.clear();
				d.append("EmployeeName", "Balaji").append("EmployeeNumber", "3003").append("Age",  35);
				table.insert(d);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// Update records
		System.out.println("Update records");
  	    DDocument d2 = new DDocument();
	   	d2.put("Age", 45);
	   	DDocument updateObj = new DDocument();
	   	updateObj.put("$set", d2);
		table.update(eq("EmployeeName", "Balaji"), updateObj);
		
		// Query data
		// Directly embed Mongo queries using filters : and, or, lt, gt, etc.
		System.out.println("Query data");
		DResultSet result = table.find(or(eq("EmployeeNumber", "3003"), lt("Age", 40)));
		while (result.hasNext()) {
			DDocument d3 = result.next();
			System.out.println("Employee Name : " + d3.getString("EmployeeName"));
			System.out.println("Employee Number : " + d3.getString("EmployeeNumber"));
			System.out.println("Age : " + d3.getInteger("Age").intValue());
		}
		
		dao.close();
		
		meta.close();
			
	}
}
