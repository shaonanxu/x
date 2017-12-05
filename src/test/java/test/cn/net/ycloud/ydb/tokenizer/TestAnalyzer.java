package test.cn.net.ycloud.ydb.tokenizer;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttributeImpl;

import cn.net.ycloud.ydb.tokenizer.YlikeAnalyzer;
import x.utils.IOUtils;

public class TestAnalyzer {
	
	static StandardAnalyzer testAnalyzer = new StandardAnalyzer();
	
	static void test1() throws IOException {
		String[] text = IOUtils.readFile("/home/shaonanxu/Downloads/temp.t.1");
		StringBuilder sb = new StringBuilder();
		for(String a : text) {
			sb.append(a);
		}
		String t = sb.toString();
		long st = System.currentTimeMillis();
		for(int i=0;i<3000;i++) {
			test(4, false, t);
		}
		System.out.println("COST:" + (System.currentTimeMillis()-st));
	}
	
	static void test(int minLength, boolean isSearch, String text) throws IOException {
		YlikeAnalyzer a = new YlikeAnalyzer(minLength, isSearch);
		TokenStream ts = a.tokenStream("", new StringReader(text));
		CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
		OffsetAttribute posAtt = ts.addAttribute(OffsetAttribute.class);
		ts.reset();
		while(ts.incrementToken()) {
			System.out.println(termAtt.toString());
			System.out.println(posAtt.startOffset() + "," + posAtt.endOffset());
		}
		ts.close();
//		a.close();
	}
	
	public static void main(String[] args) throws IOException {
		test(4, false, "abcdefghijklmnopqrstuvwxyz");
//		test(4, true, "abcd");
//		test(4, false, "abcd");
//		test(8, false, "英国Q网");
		test(4, false, "1404413390");
//		test1();
	}

}
