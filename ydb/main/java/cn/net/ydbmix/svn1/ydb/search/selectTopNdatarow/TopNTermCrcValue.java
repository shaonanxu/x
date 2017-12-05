package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.Arrays;

public class TopNTermCrcValue {
//	private static int TYPE_NUM=DataRowTypeSeri.ser(DataRowType.TopNTermCrcValue);

	public TopNTermCrcValue(long[] crc, String[] cmp) {
		super();
		this.crc = crc;
		this.cmp = cmp;
	}

	@Override
	public String toString() {
		return "TopNTermCrcValue [crc=" + Arrays.toString(crc) + ", cmp="
				+ Arrays.toString(cmp) + "]";
	}

	public long[] crc;
	public String[] cmp;
	
	public TopNTermCrcValue(){}
	public TopNTermCrcValue(Object[] nst)
	{
		this.crc=(long[]) nst[0];
		this.cmp=(String[])nst[1];
	}

	
	public Object[] toArrayList() {
		return new Object[]{this.crc,this.cmp};
	}

	
}
