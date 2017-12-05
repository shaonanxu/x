package cn.net.ycloud.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class YDBHashDictBak {
	
	private final static int F = 7;
	public static YDBHashDictBak initDict(String[] terms) {
		if(terms == null || terms.length == 0) return null;
		int min = terms[0].charAt(0);
		int max = min;
		for(int i=1;i<terms.length;i ++) {
			char a = terms[i].charAt(0);
			if(a < min) min = a;
			else if(a > max) max = a;
		}
		YDBHashDictBak dict = new YDBHashDictBak(min, max);
		long t = System.currentTimeMillis();
		for(String a : terms) {
			if(a.length() < 2) continue;
			dict.addTerm(a);
		}
		System.out.println(System.currentTimeMillis()-t);
		return dict;
	}

	private int base;
	private int[] index;
	private Terms[] terms;
	private int size;
	
	private YDBHashDictBak(int min, int max) {
		this.base = min - 1;
		int len = max - base + 1;
		index = new int[len];
		this.terms = new Terms[len];
	}
	
	/**
	 * 查找文本中包含的词
	 * 采用最大匹配方式查找
	 * @param text
	 * @return 如果没有命中返回空集合（size==0）;
	 */
	public List<String> scanText(String text) {
		if(text == null || text.length() == 0) return null;
		List<String> ret = new ArrayList<>();
		char[] cs = text.toCharArray();
		for(int i=0;i<cs.length;) {
			int r = seek(cs, i);
			if(r == 0) {
				i++;
			} else {
				ret.add(text.substring(i, i+r));
				i+=r;
			}
		}
		return ret;
	}
	
	private int seek(char[] cs, int pos) {
		char c = cs[pos];
		int offset = c-base;
		if(offset > 0 && offset < this.index.length) {
			int i = this.index[offset];
			if(i > 0) 
				return this.terms[i].find(cs, pos+1);	
		}
		return 0;
	}
	
	private void addTerm(String term){
		char[] cs = term.toCharArray();
		int c0 = cs[0] - base;
		int ix = index[c0];
		if(ix == 0){
			int s = ++this.size;
			index[c0] = s;
			terms[s] = new Terms(term);
		} else {
			terms[ix].addTerm(term);
		}
	}
	
	private class Terms {
		char[][] terms;
		int size;
		Terms(String term){
			this.terms = new char[2][];
			this.terms[0] = toTermArray(term);
			this.size = 1;
		}
		
		public int find(char[] cs, final int _pos) {
			final int len = this.size;
			char[][] _terms = this.terms;
			if(len < F) {
				for(int i=len-1;i>=0;i--) {
					int r = compareSubchars(cs, _pos, _terms[i]);
					if(r == 0) {
						return _terms[i].length + 1;
					}
				}
			} else {
				char c = cs[_pos];
				int lo = 0, hi = this.size;
				int mi = (lo+hi)>>>1;
				while(lo < hi) {
					char _c = _terms[mi][0];
					if(_c == c) {
						// find first
						int j = mi+1;
						for(;j<len;j++) {
							_c = _terms[j][0];
							if(_c !=  c) break;
						} 
						if(j == len) j=len-1;
						for(;j>0;j--) {
							int r = compareSubchars(cs, _pos, _terms[j]);
							if(r == 0) {	// found
								return _terms[j].length + 1;			
							} else if(r == 1) {
								return 0;
							}
						}
						break;
					} else {
						if(c < _c) hi = mi-1;
						else if (c > _c) lo = mi+1;
						mi = (lo+hi) >>> 1;
					}
				}
			}
			return 0;
		}

		void addTerm(String term) {
			char[] cs = toTermArray(term);
			int i = this.size-1;
			int r = charsCompare(cs, 0, this.terms[i--]);
			if(r == 0) return;
			if(this.size == 1) {
				if(r == -1) {
					this.terms[1] = this.terms[0];
					this.terms[0] = cs;
				} else {
					this.terms[1] = cs;
				}
			} else {
				ensureCapacity();
				if(r > 0) {
					this.terms[this.size] = cs;
				} else {
					while(i>=0) {
						r = charsCompare(cs, 0, this.terms[i]);
						if(r > 0) {
							this.backMoveTerms(i);
							this.terms[i] = cs;
							break;
						}
						i--;
					}
					if(i < 0) {
						this.backMoveTerms(0);
						this.terms[0] = cs;
					}
				}
			}
			this.size ++;
		}
		
		private void backMoveTerms(int pos) {
			int i = this.size;
			while(i > pos) {
				this.terms[i] = this.terms[i-1];
				i--;
			}
		}

		private void ensureCapacity() {
			if(this.size == this.terms.length) {
				char[][] newTerms = new char[this.size + 2][];
				System.arraycopy(terms, 0, newTerms, 0, this.size);
				this.terms = newTerms;
			}
		}
	}
	
	private int charsCompare(char[] c1, int s1, char[] c2) {
		int l1 = c1.length-s1;
		int l2 = c2.length;
		int l = Math.min(l1, l2);
		int i=0, j=s1;
		while(i < l) {
			if(c1[j] != c2[i]) break;
			i++;
			j++;
		}
		if(i < l) {
			return c1[j] < c2[i] ? -1 : 1;
		}
		return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
	}
	
	private int compareSubchars(char[] c1, int s1, char[] c2) {
		int l1 = c1.length-s1;
		int l2 = c2.length;
		int l = Math.min(l1, l2);
		int i=0, j=s1;
		while(i < l) {
			if(c1[j] != c2[i]) break;
			i++;
			j++;
		}
		if(i < l) {
			return c1[j] < c2[i] ? -1 : 1;
		}
		return l1 < l2 ? -1 : 0;
	}
	
	private char[] toTermArray(String term) {
		char[] a = new char[term.length()-1];
		System.arraycopy(term.toCharArray(), 1, a, 0, a.length);
		return a;
	}
	
	public static void main(String[] args) {
		final YDBHashDictBak a = new YDBHashDictBak(100, 102);
		System.out.println(a.charsCompare("aabcde".toCharArray(), 1, "abc".toCharArray()));
		char[][] ts = new char[3][];
		ts[0] = "abdc".toCharArray();
		ts[1] = "abed".toCharArray();
		ts[2] = "abed".toCharArray();
		Arrays.sort(ts, new Comparator<char[]>() {
			@Override
			public int compare(char[] t1, char[] t2) {
				return a.charsCompare(t1, 0, t2);
			}
		});
//		for(char[] t : ts) {
//			System.out.println(new String(t));
//		}
//		
//		System.out.println(a.charsCompare("abce".toCharArray(), 0, "abcd".toCharArray(), 0));
	}
}
