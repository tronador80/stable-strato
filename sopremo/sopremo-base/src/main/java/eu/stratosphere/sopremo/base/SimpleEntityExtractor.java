package eu.stratosphere.sopremo.base;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Very simple entity extractor that splits text in the provided JSON field.
 * Only words with capital letters at the beginning are considered entitites.
 * Words may contain all word characters as returned by the \w character group
 * in java regex framework.
 * 
 * @author mleich
 * 
 */
@InputCardinality(1)
@OutputCardinality(min = 1, max = 1)
@Name(verb = "extractentities")
public class SimpleEntityExtractor extends
		ElementaryOperator<SimpleEntityExtractor> {

	public static final String FIELD_VALUE = "simple_entityextractor_field_value";

	public static final String SPLITFLAG = "simple_entityextractor_splitflag";

	private EvaluationExpression field;

	private boolean splitflag = false;

	@Override
	protected void configureContract(Contract contract,
			Configuration stubConfiguration, EvaluationContext context) {
		super.configureContract(contract, stubConfiguration, context);
		SopremoUtil.setObject(contract.getParameters(), FIELD_VALUE, field);
		contract.getParameters().setBoolean(SPLITFLAG, splitflag);
	}

	@Property(preferred = true)
	@Name(preposition = "for")
	public void setField(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		this.field = value.clone();
		this.field = field.replace(new InputSelection(0),
				EvaluationExpression.VALUE);
		System.out.println("set first expression " + field.toString());
	}

	// TODO splitflag currently not recognized by parser 
	@Property(flag = true)
	@Name(adjective = "split")
	public void setSplitFlag(boolean value) {
		System.out.println("Setting split flag " + value);
		splitflag = value;
	}

	public static class Implementation extends SopremoMap {

		private EvaluationExpression field;

		private boolean splitflag = false;

		private Pattern pattern = Pattern.compile("([A-Z]{1,1}\\w+)");

		@Override
		public void open(Configuration parameters) {
			super.open(parameters);
			field = SopremoUtil.getObject(parameters, FIELD_VALUE, null);
			splitflag = parameters.getBoolean(SPLITFLAG, false);
		}

		@Override
		protected void map(IJsonNode value, JsonCollector out) {
			IObjectNode obj = (IObjectNode) value;
			IJsonNode node = field.evaluate(value);
			String text = node.toString();
			Matcher matcher = pattern.matcher(text);
			if (splitflag) {
				while (matcher.find()) {
					String matched = matcher.group(1);
					obj.put("entity", new TextNode(matched));
					out.collect(obj);
				}
			} else {
				IArrayNode<TextNode> array = new ArrayNode<TextNode>();
				while (matcher.find()) {
					String matched = matcher.group(1);
					array.add(new TextNode(matched));
				}
				obj.put("entity", array);
				out.collect(obj);
			}
		}
	}
}
