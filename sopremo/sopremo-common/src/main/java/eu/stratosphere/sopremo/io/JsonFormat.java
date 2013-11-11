/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.io;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 */
@Name(noun = "json")
public class JsonFormat extends SopremoFileFormat {

	public static class JsonInputFormat extends SopremoInputFormat {

		private JsonParser parser;

		@Override
		public void close() throws IOException {
			super.close();
			this.parser.close();
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.io.SopremoFileFormat.SopremoInputFormat#open(eu.stratosphere.nephele.fs.FSDataInputStream
		 * , eu.stratosphere.nephele.fs.FileInputSplit)
		 */
		@Override
		protected void open(FSDataInputStream stream, FileInputSplit split) {
			try {
				this.parser = new JsonParser(new InputStreamReader(stream, this.getEncoding()));
				this.parser.setWrappingArraySkipping(true);

				if (this.parser.checkEnd())
					this.endReached();
			} catch (UnsupportedEncodingException e) {
				// cannot happen as encoding is validated in SopremoFileFormat
			}
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoInputFormat#nextValue()
		 */
		@Override
		protected IJsonNode nextValue() throws IOException {
			final IJsonNode value = this.parser.readValueAsTree();
			if (this.parser.checkEnd())
				this.endReached();
			return value;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.FileInputFormat#createInputSplits(int)
		 */
		@Override
		public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			final Path path = this.filePath;
			final FileSystem fs = path.getFileSystem();
			final FileStatus pathFile = fs.getFileStatus(path);

			if (pathFile.isDir()) {
				// input is directory. list all contained files
				final FileStatus[] files = fs.listStatus(path);
				final FileInputSplit[] splits = new FileInputSplit[files.length];

				for (int index = 0; index < splits.length; index++) {
					final FileStatus fileStatus = files[index];
					final long len = fileStatus.getLen();
					final BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, len);
					splits[index] = new FileInputSplit(index, fileStatus.getPath(), 0, len, this.getHosts(blocks));
				}

				return splits;
			}

			final BlockLocation[] blocks = fs.getFileBlockLocations(pathFile, 0, pathFile.getLen());
			return new FileInputSplit[] { new FileInputSplit(0, pathFile.getPath(), 0, pathFile.getLen(),
				this.getHosts(blocks)) };
		}

		protected String[] getHosts(final BlockLocation[] blocks) throws IOException {
			return blocks.length > 0 ? blocks[0].getHosts() : new String[0];
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
		 */
		@Override
		public BaseStatistics getStatistics(final BaseStatistics cachedStatistics) {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.SopremoFileFormat#getOutputFormat()
	 */
	@Override
	public Class<? extends SopremoOutputFormat> getOutputFormat() {
		return JsonOutputFormat.class;
	}

	/**
	 * Writes json files with Jackson. The incoming key/value pair consists of {@link PactNull} and a {@link IJsonNode}
	 * .
	 * 
	 * @author Arvid Heise
	 */
	public static class JsonOutputFormat extends SopremoOutputFormat {

		private JsonGenerator generator;

		@Override
		public void close() throws IOException {
			this.generator.writeEndArray();
			this.generator.close();
			super.close();
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.io.SopremoFileFormat.SopremoOutputFormat#open(eu.stratosphere.nephele.fs.
		 * FSDataOutputStream, int)
		 */
		@Override
		protected void open(FSDataOutputStream stream, int taskNumber) throws IOException {
			this.generator = new JsonGenerator(new OutputStreamWriter(stream, this.getEncoding()));
			this.generator.writeStartArray();
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.io.SopremoFileFormat.SopremoOutputFormat#writeValue(eu.stratosphere.sopremo.type.
		 * IJsonNode)
		 */
		@Override
		protected void writeValue(IJsonNode value) throws IOException {
			this.generator.writeTree(value);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.SopremoFileFormat#getPreferredFilenameExtension()
	 */
	@Override
	protected String getPreferredFilenameExtension() {
		return "json";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.SopremoFileFormat#getInputFormat()
	 */
	@Override
	public Class<? extends SopremoInputFormat> getInputFormat() {
		return JsonInputFormat.class;
	}
}
