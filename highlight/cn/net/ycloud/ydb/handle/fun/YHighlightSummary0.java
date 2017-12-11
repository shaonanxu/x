package cn.net.ycloud.ydb.handle.fun;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import cn.net.ycloud.ydb.tokenizer.highlight.YdbHighlighter;

public class YHighlightSummary0 extends GenericUDF{
	
	private StringObjectInspector keyInspector;
	private StringObjectInspector contentInspector;
	private StringObjectInspector preTagInspector;
	private StringObjectInspector postTagInspector;
	private StringObjectInspector splitInspector;
	private IntObjectInspector contextMLInspector;
	private IntObjectInspector summaryLInspector;

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		LazyString _key = (LazyString) arg0[0].get();
		LazyString _content = (LazyString) arg0[1].get();
		LazyString _preTag = (LazyString) arg0[2].get();
		LazyString _postTag = (LazyString) arg0[3].get();
		LazyString _split = (LazyString) arg0[4].get();
		LazyInteger _contextMinLength = (LazyInteger) arg0[5].get();
		LazyInteger _summaryLength = (LazyInteger) arg0[6].get();
		
		String key = this.keyInspector.getPrimitiveJavaObject(_key);
		String content = this.contentInspector.getPrimitiveJavaObject(_content);
		String preTag = this.preTagInspector.getPrimitiveJavaObject(_preTag);
		String postTag = this.postTagInspector.getPrimitiveJavaObject(_postTag);
		String split = this.splitInspector.getPrimitiveJavaObject(_split);
		int contextMinLength = (int) this.contextMLInspector.getPrimitiveJavaObject(_contextMinLength);
		int summaryLength = (int) this.summaryLInspector.getPrimitiveJavaObject(_summaryLength);
		return YdbHighlighter.summaryHighlight(key, content, preTag, postTag, split, contextMinLength, summaryLength);
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return (arg0 == null || arg0[0] == null) ? "" : arg0[0];
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		this.keyInspector 		= (StringObjectInspector) arg0[0];
		this.contentInspector 	= (StringObjectInspector) arg0[1];
		this.preTagInspector  	= (StringObjectInspector) arg0[2];
		this.postTagInspector 	= (StringObjectInspector) arg0[3];
		this.splitInspector   	= (StringObjectInspector) arg0[4];
		this.contextMLInspector	= (IntObjectInspector) arg0[5];
		this.summaryLInspector	= (IntObjectInspector) arg0[6];
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

}
