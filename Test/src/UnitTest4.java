/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 27-Feb-2016
 * File Name : Test2.java
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dilshad Mustafa
 *
 */
public class UnitTest4 {
	static Logger log;
	public static void main(String args[]) {
	       System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
	        System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
	        System.setProperty("org.slf4j.simpleLogger.levelInBrackets", "true");       
	        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS Z");
	  		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");		
	  		System.setProperty("org.slf4j.simpleLogger.showLogName", "true");		

			final Logger log = LoggerFactory.getLogger(UnitTest4.class);
			UnitTest4.log = log;
			System.out.println("hdfjkv \'dfdgdf\'dgdf");
			try {
				
				throw new Exception();
			} catch (Throwable e) {
				
				
				
				log.debug(e.toString());
				//log.debug(" Cause : " + e.getCause().toString());
				StackTraceElement stacka[] = e.getStackTrace();
				for (StackTraceElement stack : stacka ) {
					log.debug(" StackTrace : " + stack.toString());
				}
			}
			
			String a[] = new String[10];
			a[0] = "hwedf";
			a[6] = "wdeeg";
			log.debug("a.toString() : {}", a.toString());
			
			
			/*
			List<Integer> list = new ArrayList<Integer>();
			for (Integer i : list) {
				
				System.out.println(i);
			}
			System.out.println("ok");
			try {
					//Test2.execute();
			} catch (Exception e) { e.printStackTrace(); }
			*/
	}
	
	public static void execute() throws ClientProtocolException, IOException, InterruptedException, ExecutionException {
		/*
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        try {
            httpclient.start();
            HttpGet request = new HttpGet("http://www.apache.org/");
            Future<HttpResponse> future = httpclient.execute(request, null);
            HttpResponse response = future.get();
            System.out.println("Response: " + response.getStatusLine());
            System.out.println("Shutting down");
        } finally {
            httpclient.close();
        }
        System.out.println("Done");
		*/
		
		/*
		try (CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault()) {
		    httpclient.start();
		    HttpPost request = new HttpPost(addr);
		    StringEntity entity = new StringEntity(event, ContentType.create("application/json", Consts.UTF_8));
		    request.setEntity(entity);
		    httpclient.execute(request, null);
		} catch (Exception e) {
		    LOG.error("Failed to sending event", e);
		}
		*/
		//Asserts a;
        CloseableHttpAsyncClient m_httpClient = HttpAsyncClients.createDefault();
       
        m_httpClient.start();

        HttpHost m_target = new HttpHost("localhost", 5000, "http");
		//HttpPost postRequest = new HttpPost("http://localhost:5000/hello");
		HttpPost postRequest = new HttpPost("/");
		
	    StringEntity params = new StringEntity("");
	    
	    postRequest.addHeader("content-type", "application/json");
	    postRequest.setEntity(params);
	            			
		log.debug("execute() executing request to " + m_target);

		//HttpAsyncRequestConsumer<HttpRequest> gh;
		
		// works HttpResponse httpResponse = httpClient.execute(target, getRequest);
		Future<HttpResponse> future = m_httpClient.execute(m_target, postRequest, null);
		//Future<HttpResponse> future = m_httpClient.execute(postRequest, null);
        //HttpResponse httpResponse = future.get();
		while(future.isDone() == false) {
			log.debug("Inside while");
		}
        HttpResponse httpResponse = null;
		try {
			httpResponse = future.get(100, TimeUnit.NANOSECONDS);
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		HttpEntity entity = httpResponse.getEntity();
	
		log.debug("execute()----------------------------------------");
		log.debug("execute() {}",httpResponse.getStatusLine());
		Header[] headers = httpResponse.getAllHeaders();
		for (int i = 0; i < headers.length; i++) {
			log.debug("execute() {}", headers[i]);
		}
		log.debug("execute()----------------------------------------");

		String jsonString = null;
		if (entity != null) {
			jsonString = EntityUtils.toString(entity);
			log.debug("execute() {}", jsonString);
		}

	}

}
