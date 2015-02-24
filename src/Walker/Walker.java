package Walker;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A walker based off the python version I made
 * Java didn't have a good walker built in
 * @author Aaron Hanich
 *
 */
public class Walker {
	private File m_topLevel;
	private FileFilter m_fileFilter;
	private FilenameFilter m_fileNameFilter;
	private FileStructure m_fileStructure;
	
	public Walker(File topLevel)
	{ 
		m_topLevel = topLevel;
		m_fileFilter = null;
		m_fileNameFilter = null;
		m_fileStructure = new FileStructure();
	}
	
	public Walker(String topLevel)
	{ this(new File(topLevel)); }
	
	/**
	 * Adds a fileFilter to the walker
	 * I did it this way because I was too lazy to build a bunch of different constructors
	 * @param ff a defined FileFilter
	 */
	public void addFileFilter(FileFilter ff)
	{
		m_fileFilter = ff;
		if (m_fileNameFilter != null)
		{
			System.out.println("Cannot contain fileNameFilter and a fileFilter");
			m_fileNameFilter = null;
		}
	}
	
	public void addFileNameFilter(FilenameFilter fnf)
	{
		m_fileNameFilter = fnf;
		if (m_fileFilter != null)
		{
			System.out.println("Cannot contain fileNameFilter and a fileFilter");
			m_fileFilter = null;
		}
	}
	
	/**
	 * Walk behaves more like pyhton's os.walk than the walker I built around it
	 * again this was laziness.  Is a breadth first search modifications could include max depth
	 * @return File[] containing files that fit the filter
	 */
	public File[] walk()
	{
		DirectoryFilter df = new DirectoryFilter();
		Queue<File> dirQueue = new LinkedBlockingQueue<File>();
		dirQueue.addAll(Arrays.asList(m_topLevel.listFiles(df)));
		m_fileStructure.addDirs(m_topLevel.listFiles(df));
		m_fileStructure.addFiles(walkFiles(m_topLevel));
		
		while (!dirQueue.isEmpty()){
			File d = dirQueue.poll();
			dirQueue.addAll(Arrays.asList(d.listFiles(df)));
			m_fileStructure.addDirs(d.listFiles(df));
			m_fileStructure.addFiles(walkFiles(d));
		}
		
		return m_fileStructure.getFiles();
	}
	
	public File[] getFiles()
	{ return m_fileStructure.getFiles(); }
	
	public File[] getDirs()
	{ return m_fileStructure.getDirs(); }
	
	/**
	 * in order to have cleaner code I broke this off it just determines which filter to go with
	 * @param dir
	 * @return File[] of files in dir that fit the filefilter
	 */
	private File[] walkFiles(File dir)
	{
		File[] files;
		BaseFileFilter ff = new BaseFileFilter();
		
		if (m_fileFilter == null && m_fileNameFilter == null){
			files = dir.listFiles(ff);
		}
		else if (m_fileFilter != null){
			files = dir.listFiles(m_fileFilter);
		}
		else {
			files = dir.listFiles(m_fileNameFilter);
		}
		
		return files;
	}
	
	class DirectoryFilter implements FileFilter
	{
		//not used yet but could be used to add directory filters as well
		String m_filterRE;
		String m_excludeRE;
		public DirectoryFilter(String filterRE, String excludeRE){
			m_filterRE = filterRE;
			m_excludeRE = excludeRE;
		}
		public DirectoryFilter(String RE){
			this(RE, "(?!)");
		}
		public DirectoryFilter() {
			this(".", "(?!)");
		}
		
		public boolean accept(File pathname)
		{
			return pathname.isDirectory();
		}
	}
	class BaseFileFilter implements FileFilter
	{
		public boolean accept(File pathname)
		{
			return pathname.isFile();
		}
	}
}

/**
 * A nice way to organize the file structure
 * @author a5617
 *
 */
class FileStructure
{
	private List<File> m_dirs;
	private List<File> m_files;
	
	public FileStructure()
	{ this(new LinkedList<File>(), new LinkedList<File>());	}
	
	public FileStructure(List<File> dirs, List<File> files)
	{
		m_dirs = new LinkedList<File>(dirs);
		m_files = new LinkedList<File>(files);
	}
	
	public File[] getFiles()
	{
		File[] f = new File[m_files.size()];
		return m_files.toArray(f);
	}
	
	public File[] getDirs()
	{
		File[] f = new File[m_dirs.size()];
		return m_dirs.toArray(f);
	}
	
	public void addFiles(File[] files)
	{ addFiles(Arrays.asList(files)); }
	
	public void addFiles(List<File> files)
	{
		m_files.addAll(files);
	}
	
	public void addDirs(File[] dirs)
	{ addDirs(Arrays.asList(dirs)); }
	
	public void addDirs(List<File> dirs)
	{
		m_dirs.addAll(dirs);
	}
}
