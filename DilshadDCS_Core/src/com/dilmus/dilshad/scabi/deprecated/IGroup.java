/**
 * @author Dilshad Mustafa
 * (c) Dilshad Mustafa
 * All Rights Reserved.
 * Created 17-Sep-2016
 * File Name : IGroup.java
 */
package com.dilmus.dilshad.scabi.deprecated;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.dilmus.dilshad.scabi.core.DataContext;
import com.dilmus.dilshad.scabi.core.DataElement;

/**
 * @author Dilshad Mustafa
 *
 */
public interface IGroup {

	// this method can be overridden by end users to create more complex groupings
	public List<String> groupValues(DataElement e, DataContext c) throws IOException;
	
}
