/**
 * 
 */
package eu.stratosphere.meteor.client.util;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;

import com.google.common.base.Charsets;

/**
 * Static class to get input of files. Load files by given relative path and read from that.
 *
 * @author André Greiner-Petter
 *
 */
public abstract class LocalTestsFileSystem {
	/**
	 * Open file by given resource path. This path relative and looks like
	 * 'TestScripts/<file name with file ending>'
	 * 
	 * @param resource relative path to test file
	 * @return input of test file
	 */
	public static String getFileInput( String resource ){	
		String input = new String();
		
		try { // try to catch all errors
			// get file path to resource
			String resourcePath = getResourcePath( resource );
			
			// create path object to file
			Path path = FileSystems.getDefault().getPath( resourcePath );
			
			// read all lines use java-7-Files to read in an efficient way
			List<String> inputLines = Files.readAllLines( path, Charsets.UTF_8 );
			
			// organize lines into string
			for ( String line : inputLines ) {
				input += line + "\n";
			}
		} catch ( IllegalArgumentException iae ) { // cannot found file
			System.err.println( "No resources found at: " + resource );
			iae.printStackTrace();
		} catch ( IllegalStateException ise ) { // cannot walk through directory tree
			System.err.println( "Cannot get resources from: " + resource );
			System.err.println();
			ise.printStackTrace();
		} catch ( SecurityException se ) { // absence of rights to read from file
			System.err.println( "You need the rights to read from file: " + resource );
			System.err.println();
			se.printStackTrace();
		} catch ( IOException ioe ) { // cannot read from file
			System.err.println( "An error has occurred. Cannot read from file: " + resource );
			System.err.println();
			ioe.printStackTrace();
		}
		
		// return input
		return input;
	}
	
	/**
	 * Get compiled file by given relative path. Find any file in class folders with 'resource' in path
	 * and returns the absolute path to this file.
	 * @param resource relative path to file
	 * @return absolute path to file
	 * @throws IllegalArgumentException if cannot search through the directory tree
	 * @throws IllegalStateException if cannot find the file with relative path given by resource
	 */
	public static String getResourcePath( String resource ) throws IllegalArgumentException, IllegalStateException {
		try {
			// get all resources by given resource name
			Enumeration<URL> resources = LocalTestsFileSystem.class.getClassLoader().getResources(resource);
			
			// gets first (and only) result
			if (resources.hasMoreElements()) {
				// cut first / from /-separated path to convert it to Path object
				return resources.nextElement().getPath().substring(1);
			}
		
			// if cannot load URLs throws an IllegalStateException of wrong resource
		} catch (IOException e) { throw new IllegalStateException(e); }
		
		// if nothing found throws a IllegalArgumentException
		throw new IllegalArgumentException("no resources found");
	}
	
}
