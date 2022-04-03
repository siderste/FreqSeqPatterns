package gr.unipi.ds.msc;

import gr.unipi.ds.msc.analysis.PDatasetAnalysis;
import gr.unipi.ds.msc.utils.enums.AnalysisError;
import gr.unipi.ds.msc.utils.enums.InputArgument;
import gr.unipi.ds.msc.utils.exception.AnalysisException;

/**
 * The class that contains the entry point of the job
 */
public class App {

	public static void main(String[] args) throws Exception {
		//Initialize the job params with default values
		String pathToInput = "";
		String pathToOutput = "";
		for (int i = 0; i < args.length; i+=2) {
			InputArgument inputArgument = InputArgument.fromValue(args[i]);
			InputArgument shortInputArgument = InputArgument.fromSortValue(args[i]);
			if (inputArgument == null && shortInputArgument == null) {
				throw new AnalysisException(AnalysisError.ARGUMENT_MISSPELLED_ERROR);
			}
			if (inputArgument == InputArgument.PATH_TO_INPUT || shortInputArgument == InputArgument.PATH_TO_INPUT) {
				pathToInput = args[i + 1];
			System.out.println("path to input: " + pathToInput);
			} else if (inputArgument == InputArgument.PATH_TO_OUTPUT || shortInputArgument == InputArgument.PATH_TO_OUTPUT) {
				pathToOutput = args[i + 1];
				System.out.println("path to output: " + pathToOutput);
			}
		}
		int outputNumber = 50;
		long neighborDistance = 3;
		double cellSizeInDegrees = 0.05d;
		double timeStepSize = 1d;
		if (args.length % 2 != 0) {
			throw new AnalysisException(AnalysisError.WRONG_NUMBER_OF_ARGUMENTS_ERROR);
		} else {
			for (int i = 0; i < args.length; i+=2) {
				
				InputArgument inputArgument = InputArgument.fromValue(args[i]);
				InputArgument shortInputArgument = InputArgument.fromSortValue(args[i]);
				if (inputArgument == null && shortInputArgument == null) {
					throw new AnalysisException(AnalysisError.ARGUMENT_MISSPELLED_ERROR);
				}
				if (inputArgument == InputArgument.PATH_TO_INPUT || shortInputArgument == InputArgument.PATH_TO_INPUT) {
					pathToInput = args[i + 1];
					System.out.println("path to input: " + pathToInput);
				} else if (inputArgument == InputArgument.PATH_TO_OUTPUT || shortInputArgument == InputArgument.PATH_TO_OUTPUT) {
					pathToOutput = args[i + 1];
					System.out.println("path to output: " + pathToOutput);
				} else if (inputArgument == InputArgument.CELL_SIZE_IN_DEGREES || shortInputArgument == InputArgument.CELL_SIZE_IN_DEGREES) {
					try {
						cellSizeInDegrees = Double.parseDouble(args[i + 1]);
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				} else if (inputArgument == InputArgument.CELL_TIME_SIZE_IN_DAYS || shortInputArgument == InputArgument.CELL_TIME_SIZE_IN_DAYS) {
					try {
						timeStepSize = Double.parseDouble(args[i + 1]);
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				} else if (inputArgument == InputArgument.TOP_K_PRINTED || shortInputArgument == InputArgument.TOP_K_PRINTED) {
					try {
						outputNumber = Integer.parseInt(args[i + 1]);
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
					if (outputNumber < 10) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				} else if (inputArgument == InputArgument.NEIGHBOR_DISTANCE || shortInputArgument == InputArgument.NEIGHBOR_DISTANCE) {
					try {
						neighborDistance = Long.parseLong(args[i + 1]);
						if (neighborDistance < 1L) {
							throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
						}
					} catch (Exception e) {
						throw new AnalysisException(AnalysisError.INPUT_NOT_SUPPORTED_ERROR);
					}
				}
			}
			PDatasetAnalysis.analyze(pathToInput, pathToOutput, cellSizeInDegrees, timeStepSize, outputNumber, neighborDistance);
		}
	}
}

/*
From the Project Structure window, navigate to Artifacts > the plus symbol + > JAR > From modules with dependencies...
In the Create JAR from Modules window, select the folder icon in the Main Class text box.
In the Create JAR from Modules window, ensure the extract to the target JAR option is selected, and then select OK.
	This setting creates a single JAR with all dependencies NOT the provided.
To create the jar, navigate to Build > Build Artifacts > Build. The project will compile in about 30 seconds. The output jar is created under \out\artifacts.
then on local spark ...
bin/spark-class org.apache.spark.deploy.master.Master
bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.2.3:7077
bin/spark-submit --master spark://192.168.2.3:7077 --class gr.unipi.ds.msc.App /home/stylianos/Desktop/disk/infolab/IdeaProjects/FreqSeqPatterns/target/scala-2.11/FreqSeqPatterns-assembly-1.0.jar --input /home/stylianos/Desktop/disk/infolab/IdeaProjects/FreqSeqPatterns/input --output /home/stylianos/Desktop/disk/infolab/IdeaProjects/FreqSeqPatterns/output/result.csv

 */