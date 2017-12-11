package cn.net.ycloud.ydb.tokenizer.highlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

class YdbHighlighter0 {
	
	public static final int DEFAULT_MAX_CHARS_TO_ANALYZE = 50*1024;
	
	private Fragmenter textFragmenter;
	private Formatter formatter;
	private Encoder encoder = new Encoder() {
		@Override
		public String encodeText(String originalText) {
			return originalText;
		}
	};
	private Set<String> tokenSet;
	private int hitsNum;
	private int hitsLength;
	
	public YdbHighlighter0(Fragmenter textFragmenter, Formatter formatter, Set<String> tokenSet) {
		this.textFragmenter = textFragmenter;
		this.formatter = formatter;
		this.tokenSet = tokenSet;
	}
	
	public final TextFragment[] getBestTextFragments(TokenStream tokenStream, String text, int maxNumFragments) throws IOException {
		List<TextFragment> docFrags = new ArrayList<>();
		StringBuilder newText = new StringBuilder();

		CharTermAttribute termAtt = tokenStream.addAttribute(CharTermAttribute.class);
		OffsetAttribute offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
		TextFragment currentFrag = new TextFragment(newText, newText.length(), docFrags.size());
		docFrags.add(currentFrag);
		try {
			String tokenText = null;
			int startOffset;
			int endOffset;
			int lastEndOffset = 0;
			String token = null;
			textFragmenter.start(text, tokenStream);
			YdbTokenGroup tokenGroup = new YdbTokenGroup(tokenStream);
			tokenStream.reset();
			for (boolean next = tokenStream.incrementToken();
					next && (offsetAtt.startOffset() < DEFAULT_MAX_CHARS_TO_ANALYZE);
					next = tokenStream.incrementToken()){
				
				if ((offsetAtt.endOffset() > text.length()) || (offsetAtt.startOffset() > text.length())){
					throw new IOException("Token "+ termAtt.toString()+" exceeds length of provided text sized "+text.length());
				}
				if((tokenGroup.getNumTokens() > 0) && (tokenGroup.isDistinct())){
					startOffset = tokenGroup.getStartOffset();
					endOffset = tokenGroup.getEndOffset();
					tokenText = text.substring(startOffset, endOffset);
					String markedUpText = formatter.highlightTerm(encoder.encodeText(tokenText), tokenGroup);
					if (startOffset > lastEndOffset)
						newText.append(encoder.encodeText(text.substring(lastEndOffset, startOffset)));
					newText.append(markedUpText);
					lastEndOffset = Math.max(endOffset, lastEndOffset);
					currentFrag.textLength += tokenGroup.tokenLength();
					tokenGroup.clear();
					if (textFragmenter.isNewFragment()) {
						currentFrag.textEndPos = newText.length();
						currentFrag = new TextFragment(newText, newText.length(), docFrags.size());
						docFrags.add(currentFrag);
					}
				}
				token = termAtt.toString();
				boolean isHit = this.tokenSet.contains(token);
				if(isHit) {
					this.hitsNum ++;
					currentFrag.hit = isHit;
				}
				tokenGroup.addToken(isHit ? 1f : 0f);
			}

			if (tokenGroup.getNumTokens() > 0) {
				// flush the accumulated text (same code as in above loop)
				startOffset = tokenGroup.getStartOffset();
				endOffset = tokenGroup.getEndOffset();
				tokenText = text.substring(startOffset, endOffset);
				String markedUpText = formatter.highlightTerm(encoder.encodeText(tokenText), tokenGroup);
				// store any whitespace etc from between this and last group
				if (startOffset > lastEndOffset)
					newText.append(encoder.encodeText(text.substring(lastEndOffset, startOffset)));
				newText.append(markedUpText);
				lastEndOffset = Math.max(lastEndOffset, endOffset);
				currentFrag.textLength += tokenGroup.tokenLength();
			}

			if (lastEndOffset < text.length() && text.length() <= DEFAULT_MAX_CHARS_TO_ANALYZE) {
				newText.append(encoder.encodeText(text.substring(lastEndOffset)));
			}
			currentFrag.textEndPos = newText.length();
			return docFrags.toArray(new TextFragment[docFrags.size()]);
		} finally {
			if (tokenStream != null) {
				try {
					tokenStream.end();
					tokenStream.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	int hitsNum() {
		return this.hitsNum;
	}
	
	class TextFragment {
		CharSequence markedUpText;
		int textStartPos;
		int textEndPos;
		int fragNum;
		int textLength;
		boolean hit;
		
		public TextFragment(CharSequence markedUpText, int textStartPos, int fragNum) {
			this.markedUpText = markedUpText;
			this.textStartPos = textStartPos;
			this.fragNum = fragNum;
		}
		
		public boolean follows(TextFragment fragment) {
			return textStartPos == fragment.textEndPos;
		}
		
		@Override
		public String toString() {
			return markedUpText.subSequence(textStartPos, textEndPos).toString();
		}
		
		public int length() {
			return this.textLength;
		}
	}

	public interface Formatter {
		String highlightTerm(String originalText, YdbTokenGroup tokenGroup);
	}
	
	public interface Fragmenter {
		public void start(String originalText, TokenStream tokenStream);
		public boolean isNewFragment();
	}
	
	public interface Encoder {
		String encodeText(String originalText);
	}
}
