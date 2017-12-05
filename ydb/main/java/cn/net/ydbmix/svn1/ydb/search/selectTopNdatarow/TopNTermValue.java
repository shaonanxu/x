package cn.net.ydbmix.svn1.ydb.search.selectTopNdatarow;

import java.util.Arrays;

public class TopNTermValue {
//	private static int TYPE_NUM=DataRowTypeSeri.ser(DataRowType.TopNTermValue);

	public String[] row;
	
	
	public TopNTermValue(){}
	public TopNTermValue(Object[] nst)
	{
		this.row=(String[]) nst;
	}
	
	
	public Object[] toArrayList() {
		return this.row;
	}



	@Override
	public String toString() {
		return "TopNTermValue [row=" + Arrays.toString(row) + "]";
	}
}
