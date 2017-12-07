package cn.net.ydbmix.svn2.ydb.core.index;

import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SortedIntDocSet;

import cn.net.ycloud.ydb.utils.MillisecondClock;
import cn.net.ycloud.ydb.utils.UniqConfig;


public class YdbInvertDoclist {
	private static Logger LOG = Logger.getLogger("ycloud.YdbInvertDoclist");

	public static int TERM_LIMIT = 10240;
	public static int DOCSET_LIMIT = 512;

	public static interface InvertDocs {
		public long each(String termKey, PostingsEnum docsEnum, int freq, int termcount) throws IOException;
	}


	public final static class SkipEnum {
		final ConjunctionDISI impl;

		public SkipEnum(PostingsEnum docsEnum, DocSet livebits) throws IOException {

			if (livebits instanceof BitDocSet) {

				BitDocSet bitdocset = (BitDocSet) livebits;
				DocIdSetIterator iterbits = new BitSetIterator(bitdocset.getBits(), 0);

				impl = ConjunctionDISI.intersect(Arrays.asList(docsEnum, iterbits));

			} else if (livebits instanceof SortedIntDocSet) {
				SortedIntDocSet sortint = (SortedIntDocSet) livebits;

				YdbIntArrayDocIdSetIterator iterbits = new YdbIntArrayDocIdSetIterator(sortint.getDocs(), sortint.size());

				impl = ConjunctionDISI.intersect(Arrays.asList(docsEnum, iterbits));

			} else {
				throw new IOException("not supprot " + livebits.getClass().getName());
			}

		}

		public int nextDoc() throws IOException {
			return impl.nextDoc();
		}

	}

	public static boolean fetch(final IndexSchema schema, final String fieldraw, final LeafReader reader, String logkey, InvertDocs invert, int termCnt) throws IOException {
		if (!UniqConfig.INSTANCE().ydbInvert()) {
			return false;
		}

		String field = schema.viewToPhysicalField(fieldraw);

		if (YdbDocValues.isCustomField(schema, field)) {
			return false;
		}

		long ts = MillisecondClock.CLOCK.now();

		if (termCnt <= 0) {
			termCnt = TERM_LIMIT;
		}

		FieldType ft = schema.getFieldType(field);
		final Terms terms = reader.terms(field);
		if (terms == null) {
			LOG.info("non term " + field + ",logkey:" + logkey);
			return true;
		}

		final String prefixStr = TrieField.getMainValuePrefix(ft);
		final BytesRef prefix;
		if (prefixStr != null) {
			prefix = new BytesRef(prefixStr);
		} else {
			prefix = new BytesRef("");
		}

		TermsEnum termsEnum = terms.iterator();
		BytesRef term = null;
		if (termsEnum.seekCeil(prefix) == TermsEnum.SeekStatus.END) {
			termsEnum = null;
		} else {
			term = termsEnum.term();
		}

		int count = 0;

		if (termCnt < 409600) {
			boolean isOver = false;
			while (term != null) {
				if (!StringHelper.startsWith(term, prefix)) {
					break;
				}
				count++;
				if (count >= TERM_LIMIT) {
					isOver = true;
				}

				if (isOver) {
					break;
				}

				term = termsEnum.next();
			}

			if (isOver) {

				long ts2 = MillisecondClock.CLOCK.now();
				LOG.info("over field:" + field + ",count:" + count + ",ts:" + (ts2 - ts) + ",logkey:" + logkey);

				return false;
			}

			termsEnum = terms.iterator();
			term = null;
			if (termsEnum.seekCeil(prefix) == TermsEnum.SeekStatus.END) {
				termsEnum = null;
			} else {
				term = termsEnum.term();
			}

		}

		DocsEnum docsEnum = null;
		long hitcnt =0l;
		Bits livebits = new Bits.MatchAllBits(reader.maxDoc());

		StringBuffer buffer = new StringBuffer();
		int maxBuff=0;
		boolean isSetBuffer=false;
		if (LOG.isDebugEnabled()) {
			isSetBuffer=true;
		}
		while (term != null) {
			if (!StringHelper.startsWith(term, prefix)) {
				break;
			}

			int freq = termsEnum.docFreq();
			docsEnum = termsEnum.docs(livebits, docsEnum, PostingsEnum.NONE);

			String termkey = ft.indexedToReadable(term, YdbDocValues.getcharsRef()).toString();
			long hit = invert.each(termkey, docsEnum, freq, count);
			if (isSetBuffer) {
				if((maxBuff++)<256)
				{
					buffer.append(java.net.URLEncoder.encode(termkey, "utf-8")).append("=").append(freq).append("|").append(hit).append("&");
				}
			}

			hitcnt += hit;
			term = termsEnum.next();
		}
		long ts2 = MillisecondClock.CLOCK.now();
		LOG.info("nonover field:" + field + ",termCount:" + count + ",hitcnt:" + hitcnt + ",ts:" + (ts2 - ts));
		if (LOG.isDebugEnabled()) {
			LOG.debug("nonover debug field:" + field + ",termCount:" + count + ",hitcnt:" + hitcnt + ",ts:" + (ts2 - ts) + ",terms:" + buffer.toString() + ",logkey:" + logkey);
		}

		return true;

	}
}
