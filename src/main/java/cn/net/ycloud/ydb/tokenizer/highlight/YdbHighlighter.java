package cn.net.ycloud.ydb.tokenizer.highlight;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import cn.net.ycloud.ydb.tokenizer.highlight.YdbHighlighter0.Formatter;
import cn.net.ycloud.ydb.tokenizer.highlight.YdbHighlighter0.Fragmenter;
import cn.net.ycloud.ydb.tokenizer.highlight.YdbHighlighter0.TextFragment;

public class YdbHighlighter {
	
	private final static String FIELD = "f";
	private final static int MAX_FRAGMENTS = 100;
	
	/**
	 * 返回含有高亮的部分摘要内容
	 * 
	 * @param key		需要高亮的关键字		
	 * @param content	具体内容
	 * @param preTag 	高亮部分起始位置标识
	 * @param postTag 	高亮部分结束位置标识
	 * @param split 	摘要文字见的分隔符号
	 * @param length 	摘要长度     必须大于90
	 * @return  
	 * 	null 表示未能找到高亮部分
	 */
	public static String summaryHighlight(String key, String content, String preTag, String postTag, 
			String split, int contextMinLength, int summaryLength) {
		if(isNull(key) || isNull(content) || contextMinLength > summaryLength) return null;
		contextMinLength = contextMinLength/2;
		if(contextMinLength == 0) contextMinLength = 1;
		SummaryTextFrament stf = new SummaryTextFrament(contextMinLength);
		YdbHighlightAnalyzer analyzer = new YdbHighlightAnalyzer(key);
		try {
			YdbHighlighter0 hl = new YdbHighlighter0(stf, formatter(preTag, postTag), analyzer.tokenSet());
			TextFragment[] tfs = hl.getBestTextFragments(analyzer.tokenStream(FIELD, new StringReader(content)), content, MAX_FRAGMENTS);
			if(hl.hitsNum() > 0) {
				if(tfs.length == 1) {
					return tfs[0].toString();
				} else if(tfs.length > 1){
					return summary(tfs, contextMinLength, summaryLength, hl.hitsNum(), split);
				}				
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			analyzer.close();
		}
		return null;
	}
	
	private static String summary(TextFragment[] tfs, final int contextMinLength, final int summaryLength, final int hitsNum, String split) {
		int hitLength = contextMinLength*hitsNum; // 可以提前精确算出来
		int block = (summaryLength-hitLength+(contextMinLength-1))/contextMinLength;
		final int d = block/hitsNum/2;
		StringBuilder sb = new StringBuilder();
		if(d <= 0) {
			int cl = 0;
			int i = 0;
			for(;i<tfs.length;i++) {
				TextFragment tf = tfs[i];
				sb.append(tf.toString());
				cl += tf.length();
				if(cl >= summaryLength) break;
			}
			if(i<tfs.length) sb.append(split);
		} else {
			int lastHitPos = 0;
			int cl = 0;
			int i = 0;
			boolean f = true;
			do {
				TextFragment tf = tfs[i];
				if(tf.hit) {
					int j = i-lastHitPos;
					if(j > d) {
						j = i-d;
						sb.append(split);
					} else {
						j = lastHitPos;
					}
					for(;j<i;j++) {
						tf = tfs[j];
						sb.append(tf.toString());
						cl+=tf.length();
					}
					int _d = d + 1;			// +1 包含i的TextFragment
					for(;i<tfs.length&& _d-->0;i++) {
						tf = tfs[i];
						sb.append(tf.toString());
						cl+=tf.length();
						if(cl>summaryLength) {
							f = false;
							break;
						}
					}
					lastHitPos = i;
				} else {
					i++;
				}
			} while(f && i<tfs.length);
			if(i == tfs.length) {
				if(sb.length() < summaryLength) {
					for(;lastHitPos<tfs.length;lastHitPos++) {
						TextFragment tf = tfs[lastHitPos];
						cl+=tf.length();
						if(cl>summaryLength) {
							if(lastHitPos < tfs.length-1)
								sb.append(split);
							break;
						}
						sb.append(tf.toString());
					}					
				} else if(lastHitPos < i){
					sb.append(split);
				}
			} else {
				sb.append(split);
			}
		}
		return sb.toString();
	}

	/**
	 * 返回全部高亮内容
	 * @param key		需要高亮的关键字		
	 * @param content	具体内容
	 * @param preTag 	高亮部分起始位置标识
	 * @param postTag 	高亮部分结束位置标识
	 * @return  
	 * 	null 表示未能找到高亮部分
	 */
	public static String highlight(String key, String content, String preTag, String postTag) {
		return summaryHighlight(key, content, preTag, postTag, "", content.length(), content.length());
	}
	
	private static boolean isNull(String str) {
		return str == null || str.length() == 0;
	}

	private static Formatter formatter(final String _preTag, final String _postTag) {
		return new Formatter() {
			private String preTag = _preTag == null ? "<B>" : _preTag;
			private String postTag = _postTag == null ? "</B>" : _postTag;
			@Override
			public String highlightTerm(String originalText, YdbTokenGroup tokenGroup) {
				if(tokenGroup.getScore() == 0) {
					return originalText;
				}
				return new StringBuilder(this.preTag).append(originalText).append(this.postTag).toString();
			}
		};
	}
	
	static class SummaryTextFrament implements Fragmenter {
		
		private final int length;
		private int count;
		private CharTermAttribute termAttr;
		
		SummaryTextFrament(int length) {
			this.length = length;
		}
		
		@Override
		public void start(String originalText, TokenStream tokenStream) {
			termAttr = tokenStream.addAttribute(CharTermAttribute.class);
		}

		@Override
		public boolean isNewFragment() {
			count += termAttr.length();
			if(count >= length) {
				count = 0;
				return true;
			}
			return false;
		}
	}
}
