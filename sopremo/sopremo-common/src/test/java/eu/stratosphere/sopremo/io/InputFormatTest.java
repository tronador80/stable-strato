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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FormatUtil;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.io.SopremoFileFormat.SopremoInputFormat;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * 
 */
@Ignore
public class InputFormatTest {
	public static Collection<IJsonNode> readFromFile(final File file, final SopremoFileFormat format,
			final Schema schema)
			throws IOException {
		Configuration config = new Configuration();
		final EvaluationContext context = new EvaluationContext();
		context.setSchema(schema);
		context.setInputsAndOutputs(0, 1);
		SopremoUtil.setObject(config, SopremoUtil.CONTEXT, context);
		SopremoUtil.transferFieldsToConfiguration(format, SopremoFileFormat.class, config,
			format.getInputFormat(), SopremoInputFormat.class);
		final SopremoInputFormat inputFormat =
			FormatUtil.openInput(format.getInputFormat(), file.toURI().toString(), config);

		List<IJsonNode> values = new ArrayList<IJsonNode>();
		while (!inputFormat.reachedEnd()) {
			final PactRecord record = new PactRecord();
			inputFormat.nextRecord(record);
			values.add(schema.recordToJson(record).clone());
		}
		inputFormat.close();
		return values;
	}
}
