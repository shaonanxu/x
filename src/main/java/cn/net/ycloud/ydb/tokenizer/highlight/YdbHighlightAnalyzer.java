package cn.net.ycloud.ydb.tokenizer.highlight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

class YdbHighlightAnalyzer extends Analyzer{
	
	public static final int HANZI = 1, OTHER = 0, FLAG = -1;
	
	private char[] index;
	private char[][] terms;
	
	public YdbHighlightAnalyzer(String input) {
		this.init(input);
	}
	
	public Set<String> tokenSet(){
		Set<String> ret = new HashSet<>();
		for(char[] a : terms) {
			ret.add(new String(a));
		}
		return ret;
	}
	
	private void init(final String input) {
		char[] buffer = new char[input.length()];
		List<char[]> list = new ArrayList<>();
		int offset = 0, length = 0;
		char[] chars = input.toCharArray();
		int p = 0;
		char lastC = 0;
		while(p < chars.length) {
			char c = chars[p++];
			if(charType(c) == FLAG) {
				if(p > 1) {
					char[] term = new char[offset];
					System.arraycopy(buffer, 0, term, 0, offset);
					list.add(term);
					offset = 0;
					length++;
					lastC = c;
				}
				continue;
			}
			if(charType(lastC) != FLAG && charType(c) != charType(lastC)) {
				char[] term = new char[offset];
				System.arraycopy(buffer, 0, term, 0, offset);
				list.add(term);
				offset = 0;
				length++;
				buffer[offset++] = c;
				lastC = c;
				continue;
			}
			buffer[offset++] = c;
			if(p == chars.length) {
				if(c != '?' && c != '*' && c != '^' && c != ' ' && c != '&') {
					length++;
				}
				char[] term = new char[offset];
				System.arraycopy(buffer, 0, term, 0, offset);
				list.add(term);
			}
			lastC = c;
		}
		
		if(list.size() == 0) {
//			String _input = input.toLowerCase();
			String _input = input;
			this.index = new char[] {_input.charAt(0)};
			this.terms = new char[][] {_input.toCharArray()};
		} else {
			this.index = new char[length];
			this.terms = new char[length][];
			sortList(list);
			for(int i = 0;i < list.size();i ++) {
				char[] a = list.get(i);
				this.index[i] = a[0];
				this.terms[i] = a;
			}
		}
	}
	
	private void sortList(List<char[]> list) {
		Collections.sort(list, new Comparator<char[]>() {
			@Override
			public int compare(char[] o1, char[] o2) {
				int len = Math.min(o1.length, o2.length);
				for(int i=0;i<len;i++) {
					if(o1[i] > o2[i]) return -1;
					else if(o1[i] < o2[i]) return 1;
				}
				if(o1.length > o2.length) return -1;
				else if(o1.length < o2.length) return 1;
				return 0;
			}
		});
	}
	
	private int charType(char c) {
		if(c == '?' || c == '*' || c == '^' || c == ' ')
			return FLAG;
		else if (c >= 0x4E00 && c <= 0x9FA5)
			return HANZI;
		else 
			return OTHER;
	}

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		return new TokenStreamComponents(new YdbHighlightTokenizer(this.index, this.terms));
	}
}
