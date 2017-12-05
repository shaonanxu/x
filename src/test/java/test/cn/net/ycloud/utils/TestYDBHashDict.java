package test.cn.net.ycloud.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TestCharTermAttributeImpl;

import com.sun.tools.jdeps.Analyzer;

import cn.net.ycloud.utils.YDBHashDict;

public class TestYDBHashDict {
	
	static void test0() {
		String[] terms = new String[] {"abc", "bc", "abcd", "ef", "ac", "ae"};
		YDBHashDict dict = YDBHashDict.initDict(terms);
		for(String a : dict.scanText("abcdefghiabciieae")) {
			System.out.println(a);
		}
	}
	
	static void test1() throws IOException {
		// /home/shaonanxu/Downloads/org/wltea/analyzer/dic
		// 
		String[] terms = readFile("/home/shaonanxu/Downloads/org/wltea/analyzer/dic/main2012.dic");
		String[] text = readFile("/home/shaonanxu/Downloads/temp.t");
		StringBuilder sb = new StringBuilder();
		for(String a : text) {
			sb.append(a);
		}
		System.out.println("TERMS:" + terms.length + ", TEXT:" + sb.length());
		System.out.println("RAM:" + Runtime.getRuntime().totalMemory()/1024/1024 + "==" + Runtime.getRuntime().freeMemory()/1024/1024);
		YDBHashDict dict = YDBHashDict.initDict(terms);
//		for(String cc : dict.scanText(sb.toString())) {
//			System.out.println(cc);
//		}
		long a = System.currentTimeMillis();
		System.out.println("TEXT LENGTH:" + dict.scanText(sb.toString()).size());
		System.out.println("COST:" + (System.currentTimeMillis() - a));
		System.out.println("RAM:" + Runtime.getRuntime().totalMemory()/1024/1024 + "==" + Runtime.getRuntime().freeMemory()/1024/1024);
	}
	
	static void test2() throws IOException {
		String[] text = readFile("/home/shaonanxu/Downloads/temp.t");
		StringBuilder sb = new StringBuilder();
		for(String a : text) {
			sb.append(a);
		}
		System.out.println("RAM:" + Runtime.getRuntime().totalMemory()/1024/1024 + "==" + Runtime.getRuntime().freeMemory()/1024/1024);
		SmartChineseAnalyzer sca = new SmartChineseAnalyzer();
		TokenStream ts = sca.tokenStream("", new StringReader(sb.toString()));
		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		System.out.println("RAM:" + Runtime.getRuntime().totalMemory()/1024/1024 + "==" + Runtime.getRuntime().freeMemory()/1024/1024);
		ts.reset();
		long st = System.currentTimeMillis();
		while(ts.incrementToken()) {
			
		}
		System.out.println(System.currentTimeMillis()-st);
	}
	
	
	public static String[] readFile(String path) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(new File(path)));
		String tmp = null;
		List<String> li = new ArrayList<>();
		while((tmp = br.readLine()) != null) {
			li.add(tmp);
		}
		return li.toArray(new String[li.size()]);
	}
	
	public static void main(String[] args) throws IOException {
		test1();
	}

}
