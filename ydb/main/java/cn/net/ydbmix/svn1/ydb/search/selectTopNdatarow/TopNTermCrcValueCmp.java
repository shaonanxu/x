package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.solr.request.compare.UniqTypeNum;

public class TopNTermCrcValueCmp implements Comparator<TopNTermCrcValue> {
	@Override
	public String toString() {
		return "TopNTermCrcValueCmp [isdesc=" + isdesc + ", columntype="
				+ Arrays.toString(columntype) + "]";
	}

	final boolean[] isdesc;
	boolean[] columntype;
	
	
	public TopNTermCrcValueCmp(boolean[] columntype,boolean[] _isdesc) {
		this.isdesc=_isdesc;
		this.columntype=columntype;
	}

	@Override
	public int compare(TopNTermCrcValue o1,
			TopNTermCrcValue o2) {
		return this.compareAsc(o1, o2);
	}
	
	public int compareAsc(TopNTermCrcValue o1, TopNTermCrcValue o2) {

		int cmp = UniqTypeNum.compareStrNum(o1.cmp, o2.cmp, this.columntype, this.isdesc);
		if (cmp == 0) {
			cmp = UniqTypeNum.compare(o1.crc, o2.crc, this.isdesc);// 数值型比较
		}
		return cmp;

	}

}
