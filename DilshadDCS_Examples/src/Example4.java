/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 15-Mar-2016
 * File Name : Example4.java
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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.ParseException;
import org.bson.Document;

import com.dilmus.dilshad.scabi.common.DScabiException;
import com.dilmus.dilshad.scabi.core.DMeta;
import com.dilmus.dilshad.scabi.core.DScabiClientException;
import com.dilmus.dilshad.scabi.core.Dao;
import com.dilmus.dilshad.scabi.db.DDocument;
import com.dilmus.dilshad.scabi.db.DResultSet;
import com.dilmus.dilshad.scabi.db.DTable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;

/**
 * @author Dilshad Mustafa
 *
 */
public class Example4 {

	public static void main(String args[]) throws IOException, ParseException, DScabiClientException, DScabiException, java.text.ParseException {
    	System.out.println("Example4");

		DMeta meta = new DMeta("localhost", "5000");
		Dao dao = new Dao(meta);
		
		// The below examples demonstrate accessing MongoCollection and doing
		// Map/Reduce on the MongoCollection
		
		// Create Table
		try {
			if (false == dao.tableExists("scabi:MyOrg.MyTables:Table1")) {
				System.out.println("Create Table");
				DTable t = dao.createTable("scabi:MyOrg.MyTables:Table1");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Get existing Table
		DTable table = dao.getTable("scabi:MyOrg.MyTables:Table1");
		
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
		
		// Access underlying MongoCollection
		MongoCollection<Document> c = table.getCollection();
		
		// Example to directly do Map/Reduce on the MongoCollection
		String map = "function() { for (var key in this) { emit(key, null); } }";
		String reduce = "function(key, s) { if (\"Age\" == key) return true; else return false; }";
		
		MapReduceIterable<Document> out = c.mapReduce(map, reduce);
		for (Document o : out) {
		    System.out.println("Key name is : " + o.get("_id").toString());
			System.out.println(o.toString());
		}			

		dao.close();
		meta.close();
			
	}

}
