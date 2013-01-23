package de.raulin.rosario.helloHBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public final class TableEntry {

	private final static Iterable<String> CSV_KEYS = Arrays
			.asList(new String[] { "timestamp", "pkey", "hidden", "alat",
					"along", "avail", "tags", "cname", "clat", "clong",
					"author", "date", "email", "love", "impression", "cat",
					"subcat", "landcode", "name", "feedback", "owner",
					"phone1", "phone2", "plz", "pqr", "service", "street",
					"updatedBy", "website" });

	private final Map<String, byte[]> values;

	public TableEntry(final String[] values) {
		this.values = new HashMap<String, byte[]>();

		final byte[] rowKey = Bytes.toBytes(values[0] + values[1]);
		this.values.put("rowkey", rowKey);

		int i = 2;
		for (final String key : CSV_KEYS) {
			final byte[] value = Bytes.toBytes(values[i]);
			this.values.put(key, value);
			++i;
		}
	}

	public void putToTable(final HTable table) throws IOException {
		final Put req = new Put(values.get("rowkey"));

		for (Map.Entry<String, byte[]> entry : values.entrySet()) {
			final byte[] key = Bytes.toBytes(entry.getKey());
			req.add(TableFiller.VALUE_FAMILY, key, entry.getValue());
		}

		req.add(TableFiller.COMMENT_FAMILY, TableFiller.COMMENT_COUNTER,
				Bytes.toBytes(0));

		table.put(req);
	}

}
