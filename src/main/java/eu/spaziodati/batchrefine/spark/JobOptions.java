package eu.spaziodati.batchrefine.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class JobOptions {

	@Option(name = "-o", usage = "specify output filename. default: no output", required = false)
	public String outputFilename;

	@Option(name = "-p", aliases = { "--partitions" }, usage = "specify number of partitions to split INPUTFILE. default: auto", required = false)
	public Integer numPartitions = 0;

	@Option(name = "-a", aliases = { "--assert" }, usage = "option to perform assertion inside ./expected folder. default: false", required = false)
	public boolean doAssert = false;

	@Argument
	public List<String> fArguments = new ArrayList<String>();

	public String getTempOutputFolder() {
		return FilenameUtils.getFullPath(fArguments.get(0)) + "../output/"
				+ FilenameUtils.getBaseName(fArguments.get(0));
	}

	public String getOutputFileName() {
		if (outputFilename == null)

			return "output/" + FilenameUtils.getBaseName(fArguments.get(1))
					+ "_" + FilenameUtils.getName(fArguments.get(0));

		else
			return outputFilename;
	}

	/**
	 * Print usage for the {@code submitJob} function
	 * 
	 */
	public void printUsageJob(CmdLineParser parser) {
		System.err.println("Usage: [OPTION...] INPUTFILE TRANSFORM\n");
		parser.printUsage(System.err);
	}
}
