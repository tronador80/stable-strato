/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.query;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.io.CsvFormat;
import eu.stratosphere.sopremo.io.JsonFormat;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.SopremoFileFormat;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;

/**
 * @author Arvid Heise
 */
public class PackageManager implements ParsingScope {
	private Map<String, PackageInfo> packages = new HashMap<String, PackageInfo>();

	private List<File> jarPathLocations = new ArrayList<File>(Arrays.asList(new File(".")));

	public final static IConfObjectRegistry<Operator<?>> IORegistry = new DefaultConfObjectRegistry<Operator<?>>();

	public final static IConfObjectRegistry<SopremoFileFormat> DefaultFormatRegistry = new DefaultConfObjectRegistry<SopremoFileFormat>();

	static {
		IORegistry.put(Sink.class);
		IORegistry.put(Source.class);

		DefaultFormatRegistry.put(CsvFormat.class);
		DefaultFormatRegistry.put(JsonFormat.class);
	}

	public PackageManager() {
		this.operatorRegistries.push(IORegistry);
		this.fileFormatRegistries.push(DefaultFormatRegistry);
	}

	private StackedConstantRegistry constantRegistries = new StackedConstantRegistry();

	private StackedFunctionRegistry functionRegistries = new StackedFunctionRegistry();

	private StackedConfObjectRegistry<Operator<?>> operatorRegistries = new StackedConfObjectRegistry<Operator<?>>();

	private StackedConfObjectRegistry<SopremoFileFormat> fileFormatRegistries = new StackedConfObjectRegistry<SopremoFileFormat>();

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar or returns a cached package structure.
	 * 
	 * @param packageName
	 */
	public PackageInfo getPackageInfo(String packageName) {
		PackageInfo packageInfo = this.packages.get(packageName);
		if (packageInfo == null) {
			File packagePath = this.findPackageInClassPath(packageName);
			if(packagePath != null)
				packageInfo = new PackageInfo(packageName, ClassLoader.getSystemClassLoader());
			else {
				packagePath = this.findPackageInJarPathLocations(packageName);
				if(packagePath ==null)
					throw new IllegalArgumentException(String.format("no package %s found", packageName));
				try {
					packageInfo = new PackageInfo(packageName, new URLClassLoader(new URL[] { packagePath.toURI().toURL() }));
				} catch (MalformedURLException e) {
					throw new IllegalStateException(e);
				}
			}
			QueryUtil.LOG.debug("adding package " + packagePath);
			try {
				packageInfo.importFrom(packagePath);
			} catch (IOException e) {
				throw new IllegalArgumentException(String.format("could not load package %s", packagePath));
			}
			this.packages.put(packageName, packageInfo);
		}
		return packageInfo;
	}

	/**
	 * Returns the names of the imported packages.
	 * 
	 * @return the packages
	 */
	public Collection<PackageInfo> getImportedPackages() {
		return this.packages.values();
	}

	/**
	 * Returns the fileFormatRegistries.
	 * 
	 * @return the fileFormatRegistries
	 */
	@Override
	public StackedConfObjectRegistry<SopremoFileFormat> getFileFormatRegistry() {
		return this.fileFormatRegistries;
	}

	/**
	 * Returns the operatorFactory.
	 * 
	 * @return the operatorFactory
	 */
	@Override
	public IConfObjectRegistry<Operator<?>> getOperatorRegistry() {
		return this.operatorRegistries;
	}

	@Override
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistries;
	}

	@Override
	public IFunctionRegistry getFunctionRegistry() {
		return this.functionRegistries;
	}

	protected File findPackageInClassPath(String packageName) {
		String classpath = System.getProperty("java.class.path");
		String sopremoPackage = "sopremo-" + packageName;
		// check in class paths
		for (String path : classpath.split(File.pathSeparator)) {
			final int pathIndex = path.indexOf(sopremoPackage);
			if (pathIndex == -1)
				continue;
			// preceding character must be a file separator
			if (pathIndex > 0 && path.charAt(pathIndex - 1) != File.separatorChar)
				continue;
			int nextIndex = pathIndex + sopremoPackage.length();
			// next character must be '.', '-', or file separator
			if (nextIndex < path.length() && path.charAt(nextIndex) != File.separatorChar
				&& path.charAt(nextIndex) != '.' && path.charAt(nextIndex) != '-')
				continue;
			return new File(path);
		}
		
		return null;
	}

	protected File findPackageInJarPathLocations(String packageName) {
		String sopremoPackage = "sopremo-" + packageName;
		// look in additional directories
		final Pattern filePattern = Pattern.compile(sopremoPackage + ".*\\.jar");
		for (File jarPathLocation : this.jarPathLocations) {
			final File[] jars = jarPathLocation.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return filePattern.matcher(name).matches();
				}
			});
			if (jars.length > 0) {
				return jars[0];
			}
		}
		return null;
	}

	/**
	 * Sets the defaultJarPath to the specified value.
	 * 
	 * @param defaultJarPath
	 *        the defaultJarPath to set
	 */
	public void addJarPathLocation(File jarPathLocation) {
		if (jarPathLocation == null)
			throw new NullPointerException("jarPathLocation must not be null");

		this.jarPathLocations.add(jarPathLocation);
	}

	/**
	 * Returns the jarPathLocations.
	 * 
	 * @return the jarPathLocations
	 */
	public List<File> getJarPathLocations() {
		return this.jarPathLocations;
	}

	public void importPackage(String packageName) {
		this.importPackage(this.getPackageInfo(packageName));
	}

	public void importPackage(PackageInfo packageInfo) {
		this.constantRegistries.push(packageInfo.getConstantRegistry());
		this.functionRegistries.push(packageInfo.getFunctionRegistry());
		this.operatorRegistries.push(packageInfo.getOperatorRegistry());
		this.fileFormatRegistries.push(packageInfo.getFileFormatRegistry());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Package manager with packages %s", this.packages);
	}
}
