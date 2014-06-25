package eu.spaziodati.batchrefine.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

/**
 * Client program that connects to RefineOnSpark Server instance, using default
 * port <b>3377</b> receives a stub using {@link Socket} connection and
 * {@link ObjectInputStream}. Afterwards, establishes an RMI call to
 * {@link RemoteInterface}
 * 
 */
public class RefineOnSparkClient {
	public static void main(String[] args) throws IOException {
		Socket getStub = null;

		try {
			getStub = new Socket("localhost", 3377);
			RemoteInterface stub = (RemoteInterface) new ObjectInputStream(
					getStub.getInputStream()).readObject();

			double time = stub.doMain(args);
			System.out.println(args[1] + "\t" + time + "s");

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
}