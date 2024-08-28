package com.ericsson.nms.dg.gen;

/**
 * Date: 26/10/13
 */
public class ChunkIndex {
	public static void main(String[] args) {
		final int totalLineCount = 3000;
		final int linesPerChunk = 1000;

		getLine(1, linesPerChunk); // CF:0 CL:1
		getLine(999, linesPerChunk); // CF:0 CF:999
		getLine(1000, linesPerChunk); // CF:1 CL:1
		getLine(1001, linesPerChunk); // CF:1 CL:2
		getLine(1999, linesPerChunk); // CF:1 CL:1000
		getLine(2000, linesPerChunk); // CF:2 CL:1
		getLine(2001, linesPerChunk);
		getLine(2999, linesPerChunk);
		getLine(3000, linesPerChunk);
		getLine(3001, linesPerChunk);
		getLine(56465, linesPerChunk);
	}

	private static void getLine(final int readLine, final int linesPerChunk) {
		int chunkLine;
		int chunkFile;
		if (readLine < linesPerChunk) {
			chunkLine = readLine % linesPerChunk;
			chunkFile = (readLine - chunkLine) / linesPerChunk;
		} else {
			chunkLine = (readLine % linesPerChunk) + 1;
			chunkFile = ((readLine - chunkLine) / linesPerChunk) + 1;
		}
		System.out.println("Line #" + readLine + " is  chunk-file:" + chunkFile + " chunk-line:" + chunkLine);
	}

}
