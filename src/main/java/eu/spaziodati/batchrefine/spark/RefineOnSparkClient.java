package eu.spaziodati.batchrefine.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import eu.spaziodati.batchrefine.spark.JobOptions;
/**
 * Client program that connects to RefineOnSpark Server instance, using default
 * port <b>3377</b> receives a stub using {@link Socket} connection and
 * {@link ObjectInputStream}. Afterwards, establishes an RMI call to
 * {@link RemoteInterface}
 * 
 */
public class RefineOnSparkClient {
	
	
	public static void main(String[] args) throws IOException {
		
		
		JobOptions options = new JobOptions();
		CmdLineParser parser = new CmdLineParser(options);
		String[] fArguments = new String[4];
		Configuration hdfsConf = new Configuration();
		try {
			parser.parseArgument(args);

			if (options.fArguments.size() < 2) {
				throw new CmdLineException(parser,
						"Two arguments are required!");
			}

			checkExists(options.fArguments.get(0));
			checkExists(options.fArguments.get(1));

		} catch (CmdLineException e) {
			options.printUsageJob(parser);
			System.exit(-1);
		} catch (FileNotFoundException e) {
			System.err.println("Check if files exist/n" + e.getMessage());
			System.exit(-1);
		}

		Socket getStub = null;

		try {
			getStub = new Socket("localhost", 3377);
			RemoteInterface stub = (RemoteInterface) new ObjectInputStream(
					getStub.getInputStream()).readObject();
			fArguments[0] = options.fArguments.get(0);
			fArguments[1] = new File(options.fArguments.get(1))
					.getAbsolutePath();
			fArguments[2] = options.getTempOutputFolder();
			fArguments[3] = options.numPartitions.toString();

			String time = stub.submitJob(fArguments);

			System.out.print(time);
			long startRetrive = System.nanoTime();

			Path resultFile = copyMergeToLocal(fArguments[2],
					new Path(options.getOutputFileName()), false, hdfsConf);

			System.out.printf("\t%2.3f",
					(System.nanoTime() - startRetrive) / 1000000000.0);

			if (options.doAssert)
				assertContentEquals(
						new File("expected/" + resultFile.getName()), new File(
								resultFile.toString()));

		} catch (IOException e) {
			System.err.println("Connectione error: Is RefineOnSpark running?\n"
					+ e.toString());
		} catch (Exception e) {
			System.err.println("Job Failed, reason: " + e.toString());
			System.out.println("failed");
		} finally {
			getStub.close();
		}
	}

	/**
	 * Method used to compare the output transform with pretransformed files
	 * line by line without loading everything into memory.
	 * 
	 * @param expectedFile
	 * @param outputFile
	 * @throws IOException
	 */

	public static void assertContentEquals(File expectedFile, File outputFile)
			throws IOException {

		BufferedReader expected = null;
		BufferedReader output = null;

		try {
			expected = new BufferedReader(new FileReader(expectedFile));
			output = new BufferedReader(new FileReader(outputFile));

			int line = 0;
			String current = null;

			do {
				current = expected.readLine();
				String actual = output.readLine();
				line++;
				if (current == null) {
					if (actual != null) {
						throw new AssertionError(
								"Actual output too short (line " + line + ").");
					}
					break;
				}

				if (!current.equals(actual)) {
					throw new AssertionError("Expected: " + current
							+ "\n Got: " + actual + "\n at line " + line);
				}

			} while (current != null);
			System.out.println("	OK");
		} catch (AssertionError e) {
			System.out.println("	NO");
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.out.println("	ERR");
		} finally {
			IOUtils.closeQuietly(expected);
			IOUtils.closeQuietly(output);
		}
	}

	/**
	 * Check exists and is a file, both works for HDFS and local files.
	 * 
	 * @param name
	 * @return File
	 * @throws IOException
	 */
	
	private static void checkExists(String name) throws IOException {
		Path file = new Path(name);
		FileSystem fileSystem = file.getFileSystem(new Configuration());
		if (fileSystem.exists(file) && fileSystem.isFile(file))
			return;
		else
			throw new FileNotFoundException(name);
	}

	private static Path copyMergeToLocal(String srcf, Path dst,
			boolean endline, Configuration conf) throws IOException {
		Path srcPath = new Path(srcf);
		FileSystem srcFs = srcPath.getFileSystem(conf);
		Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
		for (int i = 0; i < srcs.length; i++) {
			if (endline) {
				FileUtil.copyMerge(srcFs, srcs[i], FileSystem.getLocal(conf),
						dst, true, conf, "\n");
			} else {
				FileUtil.copyMerge(srcFs, srcs[i], FileSystem.getLocal(conf),
						dst, true, conf, null);
			}
		}
		return dst;
	}

}