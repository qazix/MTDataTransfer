import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;

import Utils.FilePair;
import Utils.ResultSet;


public class TransferWorker implements Callable<ResultSet>
{
	private FilePair m_work;
	private String m_logFile;
	private byte[] m_smDigest;
	private byte[] m_dmDigest;
	
	public TransferWorker(FilePair work, String logFile)
	{
		m_work = work;
		m_logFile = logFile;
	}
	
	public ResultSet call()
	{
		String result = "Failed";
		try {
			doWork();
			result = checkWork();
			if (result.equals("Failed"))
			{
				System.err.println("Reattempting after failure");
				doWork();
				checkWork();
			}
//			System.out.println("result is " + result);
		} catch (InterruptedException e) {
			System.err.println("Thread was interupted");
			return new ResultSet(m_work, "Failed");
		} catch (Exception e) {
			e.printStackTrace();
			return new ResultSet(m_work, "Failed");
		} finally {
			writeResults(m_logFile, new ResultSet(m_work, result));
		}
		return (new ResultSet(m_work, result));
		
//		System.out.println("Finished run");
	}
	
	private void doWork() throws IOException, InterruptedException, NoSuchAlgorithmException
	{
		System.out.println("Copying from Source " + m_work.m_source + "\t to Destination " + m_work.m_dest);
		File source = new File(m_work.m_source);
		File dest = new File(m_work.m_dest);
		MessageDigest smd = null;
		InputStream is = null;
		DigestInputStream dis = null;
		OutputStream os = null;

    	smd = MessageDigest.getInstance("MD5");
		is = new BufferedInputStream(new FileInputStream(source));
		dis = new DigestInputStream(is, smd);
		os = new BufferedOutputStream(new FileOutputStream(dest));

		long startTime = System.nanoTime();
		copyInputToOutput(dis, os);
		long endTime = System.nanoTime();
		System.out.println("Copying " + dest.length() / 1000000.0 + " MB file took " + (endTime - startTime) / 1000000000.0 + " seconds");
		
		m_smDigest = smd.digest();

    	if (is != null)
    		is.close();
    	if (os != null)
    		os.close();
    	if (dis != null)
    		dis.close();

	}
	
	private void copyInputToOutput(final InputStream in, final OutputStream out) throws IOException, InterruptedException
	{
		int bufSize = 1024 * 1024;
		final byte[] buf = new byte[bufSize];
		int readRes;
		while ((readRes = in.read(buf, 0, bufSize)) != -1)
		{
			if (Thread.currentThread().isInterrupted())
				throw new InterruptedException();
			out.write(buf, 0, readRes);
		}		
	}
	
	@SuppressWarnings("resource")
	private String checkWork() throws NoSuchAlgorithmException, IOException, InterruptedException
	{
		Boolean successful = true;
		int bufSize = 1024 * 1024;
		byte[] buf = new byte[bufSize];
		MessageDigest dmd = null;
		InputStream is = null;
		DigestInputStream dis = null;
		File dest = new File(m_work.m_dest);

//		System.out.println("Checking " + m_work.m_source + " is equal to " + m_work.m_dest + " their bytes are " + 
//					  	   new File(m_work.m_source).length() + " and " + new File(m_work.m_dest).length());
		
		dmd = MessageDigest.getInstance("MD5");
		is = new BufferedInputStream(new FileInputStream(dest));
		dis = new DigestInputStream(is, dmd);
		long startTime = System.nanoTime();
		while (dis.read(buf, 0, bufSize) != -1)
			if (Thread.currentThread().isInterrupted())
				throw new InterruptedException();
		
		long endTime = System.nanoTime();
		System.out.println("Checksum for " + dest.length() / 1000000.0 + " MB file took " + (endTime - startTime) / 1000000000.0 + " seconds");
		
		m_dmDigest = dmd.digest();
		
		try {
		if (is != null)
			is.close();
		if (dis != null)
			dis.close();
		} catch (Exception e) {
			return "Failed";
		}
			
		if (m_smDigest.length != m_dmDigest.length)
			successful = false;
		for (int i = 0; successful && i < m_smDigest.length; ++i)
		{
			if (m_smDigest[i] != m_dmDigest[i])
				successful = false;
		}
		
		return (successful ? "Complete" : "Failed");
	}
	
	private static synchronized void writeResults(String logFile, ResultSet rs)
	{
		PrintWriter logWriter = null;
		try {
			logWriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
			logWriter.println(rs.toString());
		}
		catch (Exception e)	{
			e.printStackTrace();
		}
		finally{
			if (logWriter != null)
				logWriter.close();
//			System.out.println("Writing Finished");
		}
	}
}