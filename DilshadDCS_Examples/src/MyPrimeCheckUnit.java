/**
 * @author Dilshad Mustafa
 * Copyright (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 06-Mar-2016
 * File Name : MyPrimeCheckUnit.java
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.dilmus.dilshad.scabi.core.DComputeContext;
import com.dilmus.dilshad.scabi.core.DComputeUnit;
import com.dilmus.dilshad.scabi.core.Dson;

/**
 * @author Dilshad Mustafa
 *
 */
public class MyPrimeCheckUnit extends DComputeUnit {

	public BigInteger sqrt(BigInteger x) {
    	
        BigInteger div = BigInteger.ZERO.setBit(x.bitLength()/2);
        BigInteger div2 = div;

        for(;;) {
            BigInteger y = div.add(x.divide(div)).shiftRight(1);
            if (y.equals(div) || y.equals(div2))
                return y;
            div2 = div;
            div = y;
        }
    }
    
	public String compute(DComputeContext jsonInput) {
    	long time1 = System.currentTimeMillis();

		long totalunits = jsonInput.getTU();
		long thisunit = jsonInput.getCU();
		Dson dson = null;
		String inputNumber = null;
		
		System.out.println("totalunits : " + totalunits);
		System.out.println("thisunit : " + thisunit);
		
		try {
			dson = jsonInput.getInput();
			inputNumber = dson.getString("NumberToCheck");
		} catch (Exception e) {
			return e.toString();
		}
		
		System.out.println("dson : " + dson);
		System.out.println("inputNumber : " + inputNumber);
    	
		BigInteger input = new BigInteger(inputNumber);
			
		BigInteger two =new BigInteger("2");
		BigInteger sqr;
		BigInteger i;

    	// chunk = sqrt(N) / TU
    	// (CU - 1) * chunk + 1 to CU * chunk

		sqr = new BigInteger(sqrt(input).toString());
    	//log.debug("sqrt of input {} is : {}", input.toString(), sqr.toString());
    	System.out.println("sqrt of input " + input.toString() + " is " + sqr.toString());
		if (input.remainder(two) == BigInteger.ZERO)
			return "false";
		i = new BigInteger("3");

		BigInteger chunk = sqr.divide(new BigInteger("" + totalunits));
		BigInteger start = new BigInteger("" + thisunit);
		start = start.subtract(BigInteger.ONE);
		start = start.multiply(chunk);
		start = start.add(BigInteger.ONE);
		
		// if one, start from 3
		if (start.compareTo(BigInteger.ONE) == 0)
			start = i;
		// make it odd number
		if (start.remainder(two) == BigInteger.ZERO)
			start = start.add(BigInteger.ONE);
		
		BigInteger stop = new BigInteger("" + thisunit);
		stop = stop.multiply(chunk);
		
		i = start;
		while (true) {
			//log.debug("i is now : {}", i.toString());
			if (i.compareTo(stop) > 0)
				break;
			if (input.remainder(i) == BigInteger.ZERO)
				return "false";
			i = i.add(two);
			
		}
    	long time2 = System.currentTimeMillis();
    	System.out.println("Time taken : " + (time2 - time1)); 

		return "true";			
	}


}
