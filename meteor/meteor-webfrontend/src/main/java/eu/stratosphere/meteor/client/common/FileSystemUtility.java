package eu.stratosphere.meteor.client.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.stratosphere.meteor.client.web.ErrorServlet;

/**
 * Utility class for accessing HDFS Files
 * 
 * @author 	mleich
 * 			edited by André Greiner-Petter
 */
public class FileSystemUtility {
	/**
	 * Not instantiable
	 */
	private FileSystemUtility(){};
	
	/** 
	 * split full hdfs path into host and path component... regex ftw 
	 * first group is host, second is path
	 */
	private static final Pattern hdfsPattern = Pattern.compile("(hdfs://[a-zA-Z0-9\\.:\\-]+)(/[a-zA-Z\\./_]+)");
	
	/**
	 * Pattern to get path to local file system
	 */
	private static final Pattern localPattern = Pattern.compile("file://(/[\\w\\./:\\-_]+)");
	
	/**
	 * Handle with file and writes all characters from file to writer
	 * @param filePath
	 * @param writer
	 * @throws Exception
	 */
	public static void getFileContent(String filePath, Writer writer) throws Exception {
		String host = null;
		String path = null;
		String localPath = null;
		
		// test hdfs path
		Matcher matcher = hdfsPattern.matcher(filePath);
		if ( matcher.find() ) {
			host = matcher.group(1);
			path = matcher.group(2);
		}
		
		// test local path
		matcher = localPattern.matcher(filePath);
		if ( matcher.find() ) {
			localPath = matcher.group(1);
		}
		
		// handle with regex informations
		if ( localPath != null ) getLocalFile( localPath, writer );
		else if ( host == null ) throw new Exception("Coud not parse HDFS path: " + filePath);
		else getHDFSFile( host, path, writer );
	}
	
	/**
	 * Reads all characters from a file (defined by path) and writes it to writer
	 * @param path
	 * @param writer
	 */
	private static void getLocalFile( String path, Writer writer ){
		File file = new File( path );
		if ( !(file.isAbsolute() && file.isFile()) ) return;
		
		try ( BufferedReader reader = new BufferedReader( new FileReader(file) ) ){
			char[] buffer = new char[ 4*1024 ];
			
			// read all characters from file and write it to writer
			int len = reader.read(buffer);
			while (len > 0) { // while there is more input
				writer.write(buffer, 0, len);
				len = reader.read(buffer);
			}
		} catch (FileNotFoundException e) {
			ErrorServlet.setError("<span class=\"error\">Cannot found file by given path:</span> " + path );
		} catch (IOException e) {
			ErrorServlet.setError("<span class=\"error\">Cannot read from file with path:</span> " + path );
		}
	}
	
	/**
	 * writes the contents of the file/directory to the passed writer
	 * if the path is a directory, all the content of all files in the directory is returned
	 * nesting is not supported, directories in directories are ignored.
	 * 
	 * @param host
	 * @param path
	 * @param writer
	 * @throws Exception
	 */
	private static void getHDFSFile( String host, String path, Writer writer ) throws Exception {
		Path pt = new Path(path);
		Configuration conf = new Configuration();
		
		final Class<?> clazz = FileSystem.getFileSystemClass("hdfs", conf);
		FileSystem fs = null;
		fs = (org.apache.hadoop.fs.FileSystem) clazz.newInstance();
		// For HDFS we have to have an authority
		URI name = URI.create(host);
		// Initialize HDFS
		fs.initialize(name, conf); 
		if (fs != null) {
			FileStatus[] files;
			if (fs.getFileStatus(pt).isDirectory()) {
				// enumerate all files in directory and merge content
				// we ignore nested paths!
				files = fs.listStatus(pt);
			} else {
				// just put the one path in the list
				files = new FileStatus[] { fs.getFileStatus(pt) };
			}
			
			char[] buffer = new char[4 * 1024];
			for (FileStatus file : files) {
				if (!file.isDirectory()) {
					System.out.println("trying to open " + file.getPath().toString());
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					int len = br.read(buffer);
					while (len > 0) {
						writer.write(buffer, 0, len);
						len = br.read(buffer);
					}
					br.close();
				}
			}
			fs.close();
			System.out.println("done reading hdfs");
		}
	}
	
}
