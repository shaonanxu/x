package cn.net.ycloud.ydb.tokenizer.highlight;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

public class YdbTokenGroup {
	
	private OffsetAttribute offsetAtt;
//	private CharTermAttribute termAtt;
	
	private int numTokens;
	private int matchStartOffset;
	private int matchEndOffset;
	private int startOffset;
	private int endOffset;
	private float score;

	public YdbTokenGroup(TokenStream tokenStream) {
		offsetAtt = tokenStream.addAttribute(OffsetAttribute.class);
//		termAtt = tokenStream.addAttribute(CharTermAttribute.class);
	}
	
	public void addToken(float score) {
		final int termStartOffset = offsetAtt.startOffset();
		final int termEndOffset = offsetAtt.endOffset();
		if (numTokens == 0) {
			startOffset = matchStartOffset = termStartOffset;
			endOffset = matchEndOffset = termEndOffset;
		} else {
			startOffset = Math.min(startOffset, termStartOffset);
			endOffset = Math.max(endOffset, termEndOffset);
			if (score > 0) {
				matchStartOffset = Math.min(matchStartOffset, termStartOffset);
				matchEndOffset = Math.max(matchEndOffset, termEndOffset);
			}
		}
		this.score += score;
		numTokens++;
	}
	
	public int getStartOffset() {
		return matchStartOffset;
	}

	public int getEndOffset() {
		return matchEndOffset;
	}

	public int getNumTokens() {
		return numTokens;
	}

	public boolean isDistinct() {
		return offsetAtt.startOffset() >= endOffset;
	}
	
	public void clear() {
	    this.numTokens = 0;
	    this.score = 0;
	}
	
	public float getScore() {
		return this.score;
	}
}
