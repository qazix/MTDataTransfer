package Utils;

public class ResultSet
{
	public FilePair m_data;
	public String m_result;
	
	public ResultSet(FilePair data, String result)
	{
		m_data = data;
		m_result = result;
	}
	
	public String toString()
	{
		return ("\"" + m_data.m_source + "\", \"" + m_data.m_dest + "\", \"" + m_result + "\"");
	}
}