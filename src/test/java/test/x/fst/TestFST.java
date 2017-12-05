package test.x.fst;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

public class TestFST {
	
	public static void main(String[] args) throws IOException {
		PositiveIntOutputs output = PositiveIntOutputs.getSingleton();
		Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, output);
		
		BytesRef a = new BytesRef("abc");
		
	    builder.add(Util.toIntsRef(a, new IntsRefBuilder()), 17L);
		
	}

}
