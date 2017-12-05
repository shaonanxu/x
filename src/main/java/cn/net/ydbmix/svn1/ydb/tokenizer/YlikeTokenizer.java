package cn.net.ydbmix.svn1.ydb.tokenizer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.util.AttributeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YlikeTokenizer extends Tokenizer {
	public static Logger LOG = LoggerFactory.getLogger("ycloud.YlikeTokenizer");
	public static int offset_inc=5000;
	public static int offset_sparn=3000;
	public YlikeTokenizer(int minlength, boolean isforsearch) {
		super();
		this.isSearch = isforsearch;
		this.splitLength = minlength;
		this.tokenlistArray = new TokenSingle[this.splitLength];
		for (int i = 0; i < this.splitLength; i++) {
			this.tokenlistArray[i] = new TokenSingle(0, -1, 0, 0);
		}
		this.init();
	}

	public YlikeTokenizer(AttributeFactory factory, int minlength, boolean isforsearch) {
		super(factory);
		this.isSearch = isforsearch;
		this.splitLength = minlength;
		this.tokenlistArray = new TokenSingle[this.splitLength];
		for (int i = 0; i < this.splitLength; i++) {
			this.tokenlistArray[i] = new TokenSingle(0, -1, 0, 0);
		}

		this.init();

	}

	public int getMinlength() {
		return splitLength;
	}

	public static AtomicInteger nextoffset=new AtomicInteger(0);
	public static Object nextoffsetLock=new Object();

	int curentrowoffset=0;
	private void init() {
		this.hasInit = false;
		int nexto=nextoffset.addAndGet(offset_inc);
		if(nexto>=1800000000){
			synchronized (nextoffsetLock) {
				nexto=nextoffset.get();
				if(nexto>=1800000000){
					LOG.info("nextoffset.set(0)");
					nextoffset.set(0);
				}
			}
		}
		this.curentrowoffset=nexto;

		this.loadNext_needsetPrefix = true;
		this.loadNext_incrementTokenSingle = 1;
		this.loadNext_isFinish = false;
		this.loadNext_hassendBuffer=false;

		this.tokenIndex.set(-1);

	}

	private void initReset() {
		this.init();

		for (int i = 0; i < this.splitLength; i++) {
			this.tokenlistArray[i].len = -1;
		}
	}

	final TokenSingle[] tokenlistArray;
	final AtomicInteger tokenIndex = new AtomicInteger(-1);
	final boolean isSearch ;

	final private int splitLength ;

	private int offset = 0, bufferIndex = 0, dataLen = 0, finalOffset = 0;
	private static final int IO_BUFFER_SIZE = 4096;

	private final CharTermAttributeImpl termbuffer = new CharTermAttributeImpl();
	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
	private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

	private final CharacterUtils charUtils = CharacterUtils.getInstance();
	private final CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);

	protected int normalize(int c) {
		return Character.toLowerCase(c);
	}

	public static class TokenSingle {
		public TokenSingle(int buffer, int len, int start, int end) {
			this.buffer = buffer;
			this.len = len;
			this.start = start;
			this.end = end;
		}

		public void set(int buffer, int len, int start, int end) {
			this.buffer = buffer;
			this.len = len;
			this.start = start;
			this.end = end;
		}

		@Override
		public String toString() {
			return "TokenSingle [buffer=" + buffer + ", len=" + len + ", start=" + start + ", end=" + end + "]";
		}

		int buffer;
		int len;
		int start;
		int end;
	}


	
	public boolean setBuffer(int endindex, int len) {
		char[] buffer = termAtt.buffer();
		int length = 0;
		int bufferpos = 0;

		int start = Integer.MAX_VALUE;
		int end = Integer.MIN_VALUE;
		int startindex = ((endindex - len) + 1);
		for (int i = startindex; i <= endindex; i++) {
			TokenSingle singlepos = tokenlistArray[i % len];
			start = Math.min(singlepos.start, start);
			end = Math.max(singlepos.end, end);
			length += singlepos.len;
			if (length >= buffer.length) {
				buffer = termAtt.resizeBuffer(2 + 2 * length);
			}

			// System.out.println(singlepos.toString());
			Character.toChars(normalize(singlepos.buffer), buffer, bufferpos); // buffer
			bufferpos += singlepos.len;
		}

		termAtt.setLength(length);
		offsetAtt.setOffset(correctOffset(start+curentrowoffset), finalOffset = correctOffset(end+curentrowoffset));
		return true;
	}

	public int getMaxLen() {
		return this.splitLength;
	}

	@Override
	public final boolean incrementToken() throws IOException {
		clearAttributes();
		if (!hasInit) {
			hasInit = true;
			for (int i = 1; i < this.splitLength; i++) {
				if (!this.loadNext()) {
					return false;
				}
			}
		}

		if (!this.loadNext()) {
			return false;
		}

		this.setBuffer(this.getPos(), this.splitLength);
		loadNext_hassendBuffer=true;
		return true;

	}

	boolean hasInit = false;
	boolean loadNext_needsetPrefix = true;
	int loadNext_incrementTokenSingle = 1;
	boolean loadNext_isFinish = false;

	boolean loadNext_hassendBuffer = false;
	
	public boolean loadNext() throws IOException {
		if (loadNext_isFinish) {
			return false;
		}
		boolean rtn;

		if (this.isSearch) {
			if (loadNext_incrementTokenSingle == 1) {
				rtn = this.incrementTokenSingle();
				if (!rtn) {
					if (loadNext_hassendBuffer) {
						loadNext_isFinish = true;
						rtn = false;
					}else{
						int lastindex=this.getLastTokenIndex();
						if (lastindex < 0) {
							loadNext_isFinish = true;
							rtn = false;
						} else {
							TokenSingle last = this.tokenlistArray[lastindex];
							if (last.len > 0) {
								this.tokenlistArray[this.getNextTokenIndex()].set('^', 1, last.start + 1, last.end + 1);
								loadNext_incrementTokenSingle++;
								rtn = true;
							} else {
								rtn = false;
								loadNext_isFinish = true;
							}
						}
					}
				}
			} else if (loadNext_incrementTokenSingle < this.splitLength) {

				if (loadNext_hassendBuffer) {
					loadNext_isFinish = true;
					rtn = false;
				} else {
					TokenSingle last = this.tokenlistArray[this.getLastTokenIndex()];
					this.tokenlistArray[this.getNextTokenIndex()].set('^', 1, last.start + 1, last.end + 1);
					loadNext_incrementTokenSingle++;
					rtn = true;
				}
			} else {

				loadNext_isFinish = true;
				rtn = false;
			}
		}else{
			if (loadNext_needsetPrefix) {
				this.tokenlistArray[this.getNextTokenIndex()].set('^', 1, 0, 1);
				rtn = true;
				loadNext_needsetPrefix = false;
			} else if (loadNext_incrementTokenSingle == 1) {
				rtn = this.incrementTokenSingle();
				if (!rtn) {
					TokenSingle last = this.tokenlistArray[this.getLastTokenIndex()];
					if (last.len > 0) {
						this.tokenlistArray[this.getNextTokenIndex()].set('^', 1, last.start + 1, last.end + 1);
						loadNext_incrementTokenSingle++;
						rtn = true;
					} else {
						rtn = false;
						loadNext_isFinish = true;
					}
				}
			} else if (loadNext_incrementTokenSingle < this.splitLength) {
				TokenSingle last = this.tokenlistArray[this.getLastTokenIndex()];
				this.tokenlistArray[this.getNextTokenIndex()].set('^', 1, last.start + 1, last.end + 1);
				loadNext_incrementTokenSingle++;
				rtn = true;
			} else {
				loadNext_isFinish = true;
				rtn = false;
			}
		}
		
		
		

		return rtn;

	}

	public boolean incrementTokenSingle() throws IOException {
		if (bufferIndex >= dataLen) {
			offset += dataLen;
			charUtils.fill(ioBuffer, input); // read supplementary char aware
												// with CharacterUtils
			if (ioBuffer.getLength() == 0) {
				dataLen = 0; // so next offset += dataLen won't decrement offset
				finalOffset = correctOffset(offset);
				return false;
			} else {
				dataLen = ioBuffer.getLength();
				bufferIndex = 0;
			}
		}
 
		final int c = charUtils.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
		final int charCount = Character.charCount(c);
		this.bufferIndex += charCount;
		int start = offset + bufferIndex - charCount;
		int end = offset + bufferIndex;
		char[] buffer = termbuffer.buffer();
		int length = Character.toChars(normalize(c), buffer, 0); // buffer it,
																	// normalized

		if ((!this.isSearch) && c == '^') {
			this.tokenlistArray[this.getNextTokenIndex()].set('\001', length, start + 1, end + 1);
		} else {
			this.tokenlistArray[this.getNextTokenIndex()].set(c, length, start + 1, end + 1);
		}
		return true;
	}

	public int getNextTokenIndex() {
		return tokenIndex.incrementAndGet() % this.splitLength;
	}

	public int getPos() {
		return tokenIndex.get();
	}

	public int getLastTokenIndex() {
		return tokenIndex.get() % this.splitLength;
	}

	@Override
	public final void end() throws IOException {
		super.end();
		this.offsetAtt.setOffset(finalOffset, finalOffset);
	}

	@Override
	public void reset() throws IOException {
		super.reset();
		this.initReset();
		this.bufferIndex = 0;
		this.offset = 0;
		this.dataLen = 0;
		this.finalOffset = 0;
		this.ioBuffer.reset(); // make sure to reset the IO buffer!!
	}

}
