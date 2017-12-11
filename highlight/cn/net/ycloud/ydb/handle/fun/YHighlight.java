package cn.net.ycloud.ydb.handle.fun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import cn.net.ycloud.ydb.tokenizer.highlight.YdbHighlighter;

public class YHighlight extends UDF{
	
	private final static Log LOG = LogFactory.getLog(YHighlight.class);
	
	public String evaluate(String key, String content, String preTag, String postTag, String split, int contextMinLength, int summaryLength) {
		if(isBlank(key)) {
			LOG.warn(" *** _FUNC_ YHighlightSummary P1 is Blank *** ");
			return "";
		} else if(contextMinLength > summaryLength) {
			LOG.warn(" *** _FUNC_ YHighlightSummary P6 must less than P7 *** ");
			return "";
		}
		return YdbHighlighter.summaryHighlight(key, content, preTag, postTag, split, contextMinLength, summaryLength);
	}
	
	public String evaluate(String key, String content, String preTag, String postTag) {
		if(isBlank(key)) {
			LOG.warn(" *** _FUNC_ YHighlightSummary P1 is Blank *** ");
			return "";
		}
		return YdbHighlighter.highlight(key, content, preTag, postTag);
	}
	
	private boolean isBlank(String str) {
		return str == null || str.trim().length() == 0;
	}

}
