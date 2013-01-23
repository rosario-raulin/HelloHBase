package de.raulin.rosario.helloHBase;

import java.math.BigInteger;
import java.util.Random;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public final class Comment implements Comparable<Comment> {
	private static final String COL_FORMAT_STRING = "comment_%04d_%s";
	private static final Random RANDGEN = new Random();

	private int number;
	private final String author;
	private final String text;
	private final long time;

	public Comment(final int number) {
		this.time = System.currentTimeMillis();
		this.number = number;
		this.author = generateRandomString();
		this.text = generateRandomString();
	}

	public Comment(final int number, final Result result) {
		this.number = number;

		final byte[] text = result.getValue(TableFiller.COMMENT_FAMILY,
				getColName("text"));
		final byte[] author = result.getValue(TableFiller.COMMENT_FAMILY,
				getColName("author"));
		final byte[] time = result.getValue(TableFiller.COMMENT_FAMILY,
				getColName("time"));

		this.author = Bytes.toString(author);
		this.text = Bytes.toString(text);
		this.time = Bytes.toLong(time);
	}

	private String generateRandomString() {
		return String.format("%d_%s", number,
				new BigInteger(130, RANDGEN).toString(32));
	}

	public Put asPutRequest(final byte[] row) {
		final Put req = new Put(row);

		final byte[] authorName = getColName("author");
		final byte[] textName = getColName("text");
		final byte[] timeName = getColName("time");

		final byte[] bTime = Bytes.toBytes(time);
		req.add(TableFiller.COMMENT_FAMILY, authorName, Bytes.toBytes(author));
		req.add(TableFiller.COMMENT_FAMILY, textName, Bytes.toBytes(text));
		req.add(TableFiller.COMMENT_FAMILY, timeName, bTime);

		return req;
	}

	private byte[] getColName(final String type) {
		return Bytes.toBytes(String.format(COL_FORMAT_STRING, number, type));
	}

	@Override
	public String toString() {
		return String.format("id: %d\nauthor: %s\ntext: %s\ntime: %d\n",
				number, author, text, time);
	}

	@Override
	public int compareTo(Comment other) {
		return (int) (time - other.time);
	}
}
