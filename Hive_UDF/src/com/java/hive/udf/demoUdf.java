package com.java.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class demoUdf extends UDF{
	
	public Text evaluate(String str,String str1,String str2){
		Text result = new Text();
		String req = str.replace(str1,str2);
		result.set(req);
		return result;
	}
	

}
