package cn.net.ycloud.ydb.tokenizer;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import cn.net.ydbmix.svn1.ydb.tokenizer.AjustAllTerm;
import cn.net.ydbmix.svn1.ydb.tokenizer.YlikeTokenizer;

public class YlikeAnalyzer extends Analyzer { // implements AjustAllTerm {
	public static Logger LOG = LoggerFactory.getLogger("ycloud.YlikeAnalyzer");

	boolean issearch;
	public YlikeAnalyzer(int minlength,boolean issearch) {
		super();
		this.minlength = minlength;
		this.issearch= issearch;
	}

	private int minlength=4;
	public int getMinlength() {
		return minlength;
	}


	public static void main(String[] args) throws Exception {
		
//		printAnalysisResult(new YlikeAnalyzer(4,false), "");

		
//		printAnalysisResult(new YlikeAnalyzer(4,false), "中^华@#%人民$共和国");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "中");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "中华");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "和^国");
		printAnalysisResult(new YlikeAnalyzer(8,false), "英国Q网");
		printAnalysisResult(new YlikeAnalyzer(8,true), "1404413390");

//
//		
//		printAnalysisResult(new YlikeAnalyzer(4,false), "1");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "12");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "123");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "1234");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "12345");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "123456");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "1234567");
//		printAnalysisResult(new YlikeAnalyzer(4,false), "^1234567^");
//
//		
//		printAnalysisResult(new YlikeAnalyzer(4,true), "中^华@#%人民$共和国");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "中");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "中华");
//		
//		printAnalysisResult(new YlikeAnalyzer(4,true), "1");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "1$");
//
//		printAnalysisResult(new YlikeAnalyzer(4,true), "12");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "123");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "1234");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "12345");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "123456");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "1234567");
//		printAnalysisResult(new YlikeAnalyzer(4,true), "^1234567^");
//		YlikeAnalyzer ylike=new YlikeAnalyzer(4,true);
//		printAnalysisResult(ylike, "691111^");
//		printAnalysisResult(ylike, "69111^");
//		printAnalysisResult(ylike, "^63339^");
//		printAnalysisResult(ylike, "^93333333^");
//
//		
//
//		String termInfo="xxx$@";
//		 String wildCardChar="$@";
//		System.out.println(termInfo.substring(0,termInfo.length()-wildCardChar.length()));

	}
	
	
	private static void printAnalysisResult(YlikeAnalyzer analyzer, String keyWord) throws Exception {
		System.out.println("###################");

        System.out.println("当前使用的分词器：" + analyzer.getClass().getSimpleName()+",keyWord:"+keyWord);

		for(int i=0;i<1;i++)
		{
			 TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(keyWord));
		     tokenStream.addAttribute(CharTermAttribute.class);
		     tokenStream.addAttribute(OffsetAttribute.class);
		
		     tokenStream.reset();
		     
		     while (tokenStream.incrementToken()) {
		         CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
		         OffsetAttribute offset = tokenStream.getAttribute(OffsetAttribute.class);
		         
		         System.out.println(new String(charTermAttribute.buffer(),0,charTermAttribute.length())+"==>result:"+offset.startOffset()+","+offset.endOffset());
		
		     }
		     
		     tokenStream.close();
		}
   	

    }
	
	YlikeTokenizer toker;

	@Override
	public String toString() {
		return "YlikeAnalyzer [length="
				+ minlength + "]";
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		this.toker=new YlikeTokenizer(this.minlength,this.issearch);
    	return  new TokenStreamComponents(toker);
	}

	public Analyzer ajust(String[] lines) throws IOException {
		return this;
	}

	public Tokenizer highlight() throws IOException {
		return new YlikeTokenizer( this.minlength,this.issearch);
	}
}
