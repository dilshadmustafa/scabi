/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 22-Sep-2016
 * File Name : DMFtpServer.java
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

package com.dilmus.dilshad.scabi.cs.D2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletContext;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;

/**
 * @author Dilshad Mustafa
 *
 */
public class DMFtpServer {

	public DMFtpServer() {
		
		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory factory = new ListenerFactory();
		factory.setPort(4999);
		serverFactory.addListener("default", factory.createListener());
		PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setFile(new File("/home/anees/testdata/myusers.properties"));
		userManagerFactory.setPasswordEncryptor(new PasswordEncryptor()
		{

		        @Override
		        public String encrypt(String password) {
		            return password;
		        }

		        @Override
		        public boolean matches(String passwordToCheck, String storedPassword) {
		            return passwordToCheck.equals(storedPassword);
		        }
		    });
		    
		    BaseUser user = new BaseUser();
		    user.setName("test");
		    user.setPassword("test");
		    user.setHomeDirectory("/home/anees/testdata");
		    List<Authority> authorities = new ArrayList<Authority>();
		    authorities.add(new WritePermission());
		    user.setAuthorities(authorities);
		    UserManager um = userManagerFactory.createUserManager();
		    try
		    {
		        um.save(user);
		    }
		    catch (FtpException e)
		    {
		        e.printStackTrace();
		    }
		    serverFactory.setUserManager(um);
		    Map<String, Ftplet> m = new HashMap<String, Ftplet>();
		    m.put("myFtplet", new Ftplet()
		    {

		        @Override
		        public void init(FtpletContext ftpletContext) throws FtpException {
		            //System.out.println("init");
		            //System.out.println("Thread id is #" + Thread.currentThread().getId());
		        }

		        @Override
		        public void destroy() {
		            //System.out.println("destroy");
		            //System.out.println("Thread id is #" + Thread.currentThread().getId());
		        }

		        @Override
		        public FtpletResult beforeCommand(FtpSession session, FtpRequest request) throws FtpException, IOException
		        {
		            //System.out.println("beforeCommand " + session.getUserArgument() + " ; " + session.toString() + " , " + request.getArgument() + " ; " + request.getCommand() + " ; " + request.getRequestLine());
		            //System.out.println("Thread id is #" + Thread.currentThread().getId());

		            return FtpletResult.DEFAULT;
		        }

		        @Override
		        public FtpletResult afterCommand(FtpSession session, FtpRequest request, FtpReply reply) throws FtpException, IOException
		        {
		            //System.out.println("afterCommand " + session.getUserArgument() + " ; " + session.toString() + " , " + request.getArgument() + " ; " + request.getCommand() + " ; " + request.getRequestLine() + " , " + reply.getMessage() + " ; " + reply.toString());
		            //System.out.println("Thread id is #" + Thread.currentThread().getId());

		            return FtpletResult.DEFAULT;
		        }

		        @Override
		        public FtpletResult onConnect(FtpSession session) throws FtpException, IOException
		        {
		            //System.out.println("onConnect " + session.getUserArgument() + " ; " + session.toString());
		            //System.out.println("Thread id is #" + Thread.currentThread().getId());

		            return FtpletResult.DEFAULT;
		        }

		        @Override
		        public FtpletResult onDisconnect(FtpSession session) throws FtpException, IOException
		        {
		            //System.out.println("onDisconnect " + session.getUserArgument() + " ; " + session.toString());
		            //System.out.println("Thread id is #" + Thread.currentThread().getId());

		            return FtpletResult.DEFAULT;
		        }
		    });
		    serverFactory.setFtplets(m);
		    
		    FtpServer server = serverFactory.createServer();
		    try
		    {
		        server.start();
		    }
		    catch (FtpException e)
		    {
		        e.printStackTrace();
		    }		
		
		
	}
	
}
