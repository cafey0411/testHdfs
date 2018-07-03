package readFileFromHDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {

	public static String getStringByTXT(String txtFilePath, Configuration conf)
	{

		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try
		{
			FileSystem fs = FileSystem.get(URI.create(txtFilePath),conf);
			fsr = fs.open(new Path(txtFilePath));
		
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));		
			while ((lineTxt = bufferedReader.readLine()) != null)
			{
				//if(lineTxt.split("\t")[0].trim().equals("00067")){
					return lineTxt;
				//}
				
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		} finally
		{
			if (bufferedReader != null)
			{
				try
				{
					bufferedReader.close();
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}

		return lineTxt;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String txtFilePath = "hdfs://192.168.1.11:9000/hello.txt";
		String mbline = getStringByTXT(txtFilePath, conf);
		System.out.println(mbline);
	}

}