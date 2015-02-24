import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import Utils.FilePair;
import Utils.ResultSet;
import Walker.Walker;


public class ILIDataTransfer {
	private String m_source;
	private String m_dest;
	private ExecutorService m_workExecutor;
	private List<String> m_logged;
	private List<Future<ResultSet>> m_copies;
 	private String m_logFile;

	/**
	 * Creates a ILIDataTransfer with threadCount equal to half of the cores because optimal multithreading is done with Ncpu or Ncpu + 1 Threads
	 * @param source
	 * @param dest
	 * @throws IOException
	 */
	public ILIDataTransfer(String source, String dest) throws IOException
	{ this(source, dest, Runtime.getRuntime().availableProcessors() + 1); }

	/**
	 * Constructs a ILIDAtaTransfer object and start and executor
	 * @param source
	 * @param dest
	 * @param threadCount
	 * @throws IOException
	 */
	public ILIDataTransfer(String source, String dest, int threadCount) throws IOException
	{
		if (!new File(source).exists() || !new File(dest).exists())
			throw new IOException("File does not exist");
		if (threadCount < 2)
			threadCount = 2;
		
		System.out.println("ThreadCount = " + threadCount);
		
		m_source = source;
		File logFile = new File(m_source, "TransferLog.txt");
		m_logged = new ArrayList<String>();
		if (logFile.exists())
		{
			BufferedReader br = new BufferedReader(new FileReader(logFile));
			String line;
			while ((line = br.readLine()) != null) 
				m_logged.add(line);
		}
		m_logFile = logFile.getAbsolutePath();
		m_copies = new ArrayList<Future<ResultSet>>();
		m_dest = dest;
		m_workExecutor = Executors.newFixedThreadPool(threadCount);
	}
	
	/**
	 * Starts and asynchronous Transfer 
	 */
	public void startTransfer()
	{
		populateQueue();
	}
	
	/**
	 * Starts a synchronous Transfer
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public void transfer() throws InterruptedException, ExecutionException
	{
		populateQueue();
		waitForTransfer();
		Boolean isComplete = checkCompletness();
		if (isComplete)
			new File(m_logFile).delete();
		else {
			privatePrintFailures();
		}
	}
	
	/**
	 * Method to wait for transfer to complete
	 * @throws InterruptedException 
	 */
	private void waitForTransfer() throws InterruptedException
	{
//		System.out.println("endTransfer");
		m_workExecutor.shutdown();
		m_workExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	}
	
	public Boolean checkCompletness() throws InterruptedException, ExecutionException
	{
		m_logged.clear();
		Boolean isComplete = true;
		
		for (Future<ResultSet> frs : m_copies) {
			if (frs.isDone() && !frs.isCancelled()) {
				ResultSet result = frs.get();
				if (!result.m_result.equals("Complete")) {
					isComplete = false;
					m_logged.add(result.toString());
				}
			}
		}
		return isComplete;
	}	
	
	/**
	 * Prematurely kills the executor
	 */
	public void endTransfer()
	{
		for (Future<ResultSet> frs : m_copies)
			frs.cancel(true);
		m_workExecutor.shutdownNow();
	}

	/**
	 * Create the file structure in the destination folder and adds workers to the executor
	 */
	private void populateQueue()
	{
		Walker walker = new Walker(m_source);
		File[] files = walker.walk();
		File[] dirs = walker.getDirs();
		String source;
		String destExt;
		Boolean inLogFile;
		
		for (int i = 0; i < dirs.length; ++i)
		{
			source = dirs[i].getAbsolutePath();
			destExt = source.substring(source.indexOf(m_source) + m_source.length());
			
			new File(m_dest + destExt).mkdirs();
		}
		
		for (int i = 0; i < files.length; ++i)
		{
			inLogFile = false;
			source = files[i].getAbsolutePath();
			destExt = source.substring(source.indexOf(m_source) + m_source.length());		
			
			//check if this has already been completed
			for (String line : m_logged)
			{
				String[] splitLine = line.split("\", \"");
				if (source.equals(splitLine[0].substring(1)) && (m_dest + destExt).equals(splitLine[1]) && splitLine[2].equals("Complete\"")) {
					inLogFile = true;
					break;
				}
			}
			
			if (!inLogFile)
				m_copies.add(m_workExecutor.submit(new TransferWorker(new FilePair(source, m_dest + destExt), m_logFile)));
		}
	}
	
	public void printFailures()
	{
		try{
		checkCompletness();
		} catch (Exception e) {
			e.printStackTrace();
		}
		privatePrintFailures();
	}
	
	private void privatePrintFailures()
	{
		for (String s : m_logged)
			System.err.println(s);
	}
	
	public static void main(String[] args)
	{
		String source = "";
		String dest = "";
		String usage = "java ILIDataTransfer <source> <dest>";
		if (args.length == 2) {
			source = args[0];
			dest = args[1];
		}
		else {
			System.err.println(usage);
			System.exit(1);
		}
		try {
			ILIDataTransfer idt = new ILIDataTransfer(source, dest);
			long startTime = System.nanoTime();
			idt.transfer();
			long endTime = System.nanoTime();
			System.out.println("Transfer Completed in " + (endTime - startTime) / 1000000000.0 + " seconds");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}