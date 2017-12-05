package cn.net.ycloud.ydb.tokenizer.highlight;

import java.io.IOException;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

class YdbHighlightTokenizer extends Tokenizer {
	
	private final char[] index;
	private final char[][] terms;
	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
	private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
	
	private final int BUFFER_LEN = 1024;
	private char[] buffer = new char[BUFFER_LEN];
	private int pos = 0;
	private int length = 0;
	private int startOffset;
	
	public YdbHighlightTokenizer(char[] index, char[][] terms) {
		this.index = index;
		this.terms = terms;
	}

	@Override
	public final boolean incrementToken() throws IOException {
		clearAttributes();
		if(this.length == 0 || pos >= BUFFER_LEN) {
			this.length = super.input.read(buffer);
			if(length == 0) return false;
			this.pos = 0;
		}
		if(this.pos >= this.length) return false;
		char c = this.buffer[this.pos];
		
		int pi = 0, pt = 0;
		while((pi = scanIndex(c, pi)) < this.index.length) {
			pt = scanTerm(pi);
			if(pt > 0) break;
			pi++;
		}
		if(pi == this.index.length || pt == -1) {
			this.termAtt.copyBuffer(buffer, this.pos++, 1);
			this.offsetAtt.setOffset(startOffset, ++startOffset);
		} else {
			this.termAtt.copyBuffer(buffer, this.pos, pt);
			int endOffset = startOffset + pt;
			this.offsetAtt.setOffset(startOffset, endOffset);
			this.pos += pt;
			startOffset = endOffset;
		}
		return true;
	}
	
	private int scanIndex(char a, int start) {
		int i = start;
		for(;i<this.index.length;i++) {
			if(this.index[i] == a) return i;
		}
		return i;
	}
	private int scanTerm(int p) {
		char[] term = this.terms[p];
		int i=1, j=pos+1;
		while(true) {
			if(i == term.length) break;
			if(j == this.length) {
				char[] newBuf = new char[BUFFER_LEN];
				System.arraycopy(this.buffer, pos, newBuf, 0, this.length-pos);
				j = this.length-pos;
				this.pos = 0;
				this.buffer = newBuf;
			}
			if(term[i++] != this.buffer[j++]) return -1;
		}
		return i==1?-1:i;
	}
}
