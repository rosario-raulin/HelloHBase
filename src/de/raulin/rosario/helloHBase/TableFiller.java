package de.raulin.rosario.helloHBase;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import au.com.bytecode.opencsv.CSVReader;

public final class TableFiller {

	private static final String TABLE_NAME = "locations";
	private final static byte[] POSTFIX = new byte[] { 0 };

	public static final byte[] COMMENT_FAMILY = Bytes.toBytes("c");
	public static final byte[] COMMENT_COUNTER = Bytes
			.toBytes("comment_counter");
	public static final byte[] VALUE_FAMILY = Bytes.toBytes("v");

	private final HTable table;

	public TableFiller() throws IOException {
		final Configuration conf = HBaseConfiguration.create();
		this.table = new HTable(conf, TABLE_NAME);
	}

	public void fillTable(final String inputPath) throws IOException {
		final CSVReader reader = new CSVReader(new FileReader(inputPath));

		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			final TableEntry entry = new TableEntry(nextLine);
			entry.putToTable(table);
		}
		reader.close();
	}

	private int getCommentNumber(final Result result) {
		return Bytes.toInt(result.getValue(COMMENT_FAMILY, COMMENT_COUNTER));
	}

	public void addComment(final int numberPerEntry) throws IOException {
		final ResultScanner scanner = table.getScanner(new Scan());

		for (final Result result : scanner) {
			final int commentNumber = getCommentNumber(result);
			final List<Put> requests = new ArrayList<Put>(numberPerEntry);

			for (int i = 0; i < numberPerEntry; ++i) {
				final Comment comment = new Comment(commentNumber + i);
				requests.add(comment.asPutRequest(result.getRow()));
			}
			requests.add(getSetCounterRequest(commentNumber + numberPerEntry,
					result.getRow()));
			table.put(requests);
		}
	}

	public Iterable<Comment> getComments(final String locName,
			final String city, final int from, final int to) throws IOException {
		final List<Comment> comments = new ArrayList<Comment>(to - from);

		final Filter locFilter = new SingleColumnValueFilter(VALUE_FAMILY,
				Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
				Bytes.toBytes(locName));

		final Filter cityFilter = new SingleColumnValueFilter(VALUE_FAMILY,
				Bytes.toBytes("cname"), CompareFilter.CompareOp.EQUAL,
				Bytes.toBytes(city));

		final byte[] min = Bytes.toBytes(String.format("comment_%04d", from));
		final byte[] max = Bytes.toBytes(String.format("comment_%04d", to + 1));
		final Filter rangeFilter = new ColumnRangeFilter(min, true, max, true);

		final List<Filter> filter = new ArrayList<Filter>(3);
		filter.add(locFilter);
		filter.add(cityFilter);
		filter.add(rangeFilter);

		final Scan scan = new Scan();
		scan.setFilter(new FilterList(filter));
		final ResultScanner scanner = table.getScanner(scan);

		for (final Result result : scanner) {
			for (int i = from; i <= to; ++i) {
				final Comment next = new Comment(i, result);
				comments.add(next);
			}
		}
		scanner.close();
		Collections.sort(comments);

		return comments;
	}

	private Put getSetCounterRequest(final int to, final byte[] row) {
		final Put req = new Put(row);
		req.add(COMMENT_FAMILY, COMMENT_COUNTER, Bytes.toBytes(to));
		return req;
	}

	public void readPagewise(final int pageSize) throws IOException {
		final List<Filter> filters = new ArrayList<Filter>(2);

		final Filter pageFilter = new PageFilter(pageSize);
		final Filter commentFilter = new ColumnPrefixFilter(
				Bytes.toBytes("comment"));
		filters.add(pageFilter);
		filters.add(commentFilter);

		final Filter combined = new FilterList(filters);

		byte[] last = null;
		while (true) {
			final Scan scan = new Scan();
			scan.setFilter(combined);
			if (last != null) {
				scan.setStartRow(Bytes.add(last, POSTFIX));
			}
			final ResultScanner scanner = table.getScanner(scan);
			boolean stop = true;
			Result result;
			while ((result = scanner.next()) != null) {
				stop = false;

				final int comments = getCommentNumber(result);
				for (int i = 0; i < comments; ++i) {
					final Comment comment = new Comment(i, result);
					System.out.println(comment);
				}
				last = result.getRow();
			}
			scanner.close();
			if (stop) {
				break;
			} else {
				System.out.println("--- page done ---");
			}
		}
	}

	public void printInterval() throws IOException {
		final Filter filter = new PrefixFilter(Bytes.toBytes("DE_BERLIN_"));
		final Scan scan = new Scan();
		scan.setFilter(filter);

		final ResultScanner scanner = table.getScanner(scan);
		for (final Result res : scanner) {
			String name = Bytes.toString(res.getValue(VALUE_FAMILY,
					Bytes.toBytes("name")));
			System.out.printf("%s: %s\n", Bytes.toString(res.getRow()), name);
		}
		scanner.close();
	}

	public static void main(String[] args) {
		if (args.length >= 1) {
			try {
				final TableFiller filler = new TableFiller();

				if (args[0].equals("fill")) {
					if (args.length >= 2) {
						filler.fillTable(args[1]);
					} else {
						printUsage();
					}
				} else if (args[0].equals("add")) {
					filler.addComment(5);
				} else if (args[0].equals("getRange")) {
					for (final Comment comment : filler.getComments(
							"Pizzeria Romano", "Berlin", 2, 4)) {
						System.out.println(comment);

					}
				} else if (args[0].equals("interval")) {
					filler.printInterval();
				} else if (args[0].equals("pagewise")) {
					filler.readPagewise(10);
				} else {
					printUsage();
				}
			} catch (final IOException e) {
				e.printStackTrace();
			}
		} else {
			printUsage();
		}
	}

	private static void printUsage() {
		System.err
				.println("usage: [fill/add/getRange/interval/pagewise] [data-input]");
	}
}
