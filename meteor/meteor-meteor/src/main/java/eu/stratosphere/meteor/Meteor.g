grammar Meteor;

options {
    language=Java;
    output=AST;
    ASTLabelType=EvaluationExpression;
    backtrack=false;
    //memoize=true;
    superClass=MeteorParserBase;
}

tokens {	
    EXPRESSION;
    OPERATOR;
}

@lexer::header { 
package eu.stratosphere.meteor; 
}

@parser::header { 
package eu.stratosphere.meteor; 

import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.io.*;
import eu.stratosphere.sopremo.query.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.function.*;
import eu.stratosphere.sopremo.type.*;
import java.math.*;
import java.util.IdentityHashMap;
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

@parser::members {
  private Stack<String> paraphrase = new Stack<String>();

  private boolean setInnerOutput(Token VAR, Operator<?> op) {
	  JsonStreamExpression output = new JsonStreamExpression(op.getOutput($objectCreation::mappings.size()));
	  $objectCreation::mappings.add(new ObjectCreation.TagMapping(output, new JsonStreamExpression(op)));
	  getVariableRegistry().getRegistry(1).put(VAR.getText(), output);
	  return true;
	}
  
  protected EvaluationExpression getInputSelection(Token inputVar) {
      return getVariable(inputVar).toInputSelection($operator::result);
  }

  public void parseSinks() throws RecognitionException {
    script();
  }
}

script
	:	 (statement ';')+ ->;

statement
	:	(assignment | operator | packageImport | functionDefinition | javaudf
// configuration function call 
	| m=methodCall[null] { $m.tree.evaluate(MissingNode.getInstance()); }) ->;
	
packageImport
  :  'using' packageName=ID { getPackageManager().importPackage($packageName.text); } 
     (',' additionalPackage=ID { getPackageManager().importPackage($additionalPackage.text); })* ->;

assignment
	:	target=VAR '=' source=operator { putVariable($target, new JsonStreamExpression($source.op)); } -> ;

functionDefinition
  : name=ID '=' func=inlineFunction { addFunction($name.text, $func.func); } -> ;
  
inlineFunction returns [ExpressionFunction func]
@init { List<Token> params = new ArrayList(); }
  : FN '('  
  (param=ID { params.add($param); }
  (',' param=ID { params.add($param); })*)? 
  ')' 
  { 
    addConstantScope();
    for(int index = 0; index < params.size(); index++) 
      this.getConstantRegistry().put(params.get(index).getText(), new InputSelection(index)); 
  } 
  def=contextAwareExpression[null] 
  { 
    $func = new ExpressionFunction(params.size(), def.tree);
    removeConstantScope(); 
  } -> ; 

javaudf
  : name=ID '=' JAVAUDF '(' path=STRING ')' 
  { addFunction($name.getText(), path.getText()); } ->;

contextAwareExpression [EvaluationExpression contextExpression]
scope { EvaluationExpression context }
@init { $contextAwareExpression::context = $contextExpression; }
  : ternaryExpression;

expression
  : (operatorExpression)=> operatorExpression
  | ternaryExpression;

ternaryExpression
	:	(orExpression '?')=> ifClause=orExpression '?' ifExpr=orExpression ':' elseExpr=orExpression
	-> ^(EXPRESSION["TernaryExpression"] $ifClause { ifExpr == null ? EvaluationExpression.VALUE : $ifExpr.tree } { $elseExpr.tree })
	| (orExpression IF)=> ifExpr2=orExpression IF ifClause2=orExpression
  -> ^(EXPRESSION["TernaryExpression"] $ifClause2 $ifExpr2 { EvaluationExpression.VALUE })
  | orExpression;
	
orExpression
  : exprs+=andExpression ((OR | '||') exprs+=andExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> { OrExpression.valueOf($exprs) };
	
andExpression
  : exprs+=elementExpression ((AND | '&&') exprs+=elementExpression)*
  -> { $exprs.size() == 1 }? { $exprs.get(0) }
  -> { AndExpression.valueOf($exprs) };
  
elementExpression
	:	elem=comparisonExpression (not=NOT? IN set=comparisonExpression)? 
	-> { set == null }? $elem
	-> ^(EXPRESSION["ElementInSetExpression"] $elem 
	{ $not == null ? ElementInSetExpression.Quantor.EXISTS_IN : ElementInSetExpression.Quantor.EXISTS_NOT_IN} $set);
	
comparisonExpression
	:	e1=arithmeticExpression ((s='<=' | s='>=' | s='<' | s='>' | s='==' | s='!=') e2=arithmeticExpression)?
	-> 	{ $s == null }? $e1
  ->  { $s.getText().equals("!=") }? ^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.NOT_EQUAL} $e2)
  ->  { $s.getText().equals("==") }? ^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.EQUAL} $e2)
	-> 	^(EXPRESSION["ComparativeExpression"] $e1 {ComparativeExpression.BinaryOperator.valueOfSymbol($s.text)} $e2);
	
arithmeticExpression
	:	e1=multiplicationExpression ((s='+' | s='-') e2=multiplicationExpression)?
	-> 	{ s != null }? ^(EXPRESSION["ArithmeticExpression"] $e1 
		{ s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION} $e2)
	-> 	$e1;
	
multiplicationExpression
	:	e1=preincrementExpression ((s='*' | s=SLASH) e2=preincrementExpression)?
	-> 	{ s != null }? ^(EXPRESSION["ArithmeticExpression"] $e1 
		{ s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION} $e2)
	-> 	$e1;
	
preincrementExpression
	:	'++' preincrementExpression
	|	'--' preincrementExpression
	|	unaryExpression;
	
unaryExpression
	:	('!' | '~')? castExpression;

castExpression
	:	('(' ID ')')=> '(' type=ID ')' expr=generalPathExpression
  -> { coerce($type.text, $expr.tree) }
	| (generalPathExpression AS)=> expr=generalPathExpression AS type=ID
  -> { coerce($type.text, $expr.tree) }
	| generalPathExpression;
	
generalPathExpression
	: value=valueExpression 
	  ((pathExpression[EvaluationExpression.VALUE])=> path=pathExpression[$value.tree] -> $path
	   | -> $value);

contextAwarePathExpression[EvaluationExpression context]
  : pathExpression[context];
  
pathExpression[EvaluationExpression inExp]
  : seg=pathSegment { ((PathSegmentExpression) seg.getTree()).setInputExpression(inExp); }
  ((pathSegment)=> path=pathExpression[$seg.tree] -> $path
   | -> $seg);

pathSegment
@init {  paraphrase.push("a path expression"); }
@after { paraphrase.pop(); }
  : // add .field or [index] to path
    ('?.')=> '?.' field=ID -> ^(EXPRESSION["ObjectAccess"] {$field.text} {true})    
  | ('.') => '.' field=ID -> ^(EXPRESSION["ObjectAccess"] {$field.text})    
  | ('[') => arrayAccess;
//    ( (pathSegment[EvaluationExpression.VALUE])=> pathSegment[$lastStep] | -> { lastStep });

arrayAccess
  : '[' STAR ']' path=pathExpression[EvaluationExpression.VALUE]
  -> ^(EXPRESSION["ArrayProjection"] $path)  
  | '[' (pos=INTEGER | pos=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($pos.text) })
  | '[' (start=INTEGER | start=UINT) ':' (end=INTEGER | end=UINT) ']' 
  -> ^(EXPRESSION["ArrayAccess"] { Integer.valueOf($start.text) } { Integer.valueOf($end.text) });
  
valueExpression
	:	(ID '(')=> methodCall[null]
	| parenthesesExpression 
	| literal 
	| (VAR '[' VAR)=> streamIndexAccess
	| VAR -> { getInputSelection($VAR) }
  | ((ID ':')=> packageName=ID ':')? constant=ID { getScope($packageName.text).getConstantRegistry().get($constant.text) != null }? => 
    -> { getScope($packageName.text).getConstantRegistry().get($constant.text) }  
	| arrayCreation 
	| objectCreation ;
	
operatorExpression
	:	op=operator -> ^(EXPRESSION["NestedOperatorExpression"] { $op.op });
		
parenthesesExpression
	:	('(' expression ')') -> expression;

methodCall [EvaluationExpression targetExpr]
@init { List<EvaluationExpression> params = new ArrayList();
        paraphrase.push("a method call"); }
@after { paraphrase.pop(); }
	:	(packageName=ID ':')? name=ID '('	
	((param=expression { params.add($param.tree); } | func=lowerOrderFunction { params.add($func.tree); }) 
	(',' (param=expression { params.add($param.tree); } | func=lowerOrderFunction { params.add($func.tree); }))*)? 
	')' -> { createCheckedMethodCall($packageName.text, $name, $targetExpr, params.toArray(new EvaluationExpression[params.size()])) };
	
lowerOrderFunction
  : '&' (packageName=ID ':')? name=ID 
        -> ^(EXPRESSION["ConstantExpression"] { new FunctionNode(getSopremoFunction($packageName.text, $name)) })
  | func=inlineFunction -> ^(EXPRESSION["ConstantExpression"] { new FunctionNode($func.func) });

fieldAssignment
	:	((ID ':')=> ID ':' expression 
    { $objectCreation::mappings.add(new ObjectCreation.FieldAssignment($ID.text, $expression.tree)); } -> )
  | VAR 
    ( '.' STAR { $objectCreation::mappings.add(new ObjectCreation.CopyFields(getInputSelection($VAR))); } ->
      | '=' op=operator { setInnerOutput($VAR, $op.op) }?=>
      | p=contextAwarePathExpression[getVariable($VAR).toInputSelection($operator::result)]
      ( ':' e2=expression { $objectCreation::mappings.add(new ObjectCreation.TagMapping($p.tree, $e2.tree)); } ->
        | /* empty */ { $objectCreation::mappings.add(new ObjectCreation.FieldAssignment(getAssignmentName($p.tree), $p.tree)); } ->
      )
    );
  catch [NoViableAltException re] { explainUsage("inside of a json object {...} only <field: expression>, <\$var.path>, <\$var = operator> or <\$var: expression> are allowed", re); }

objectCreation
scope {  List<ObjectCreation.Mapping> mappings; }
@init { $objectCreation::mappings = new ArrayList<ObjectCreation.Mapping>(); 
        paraphrase.push("a json object"); }
@after { paraphrase.pop(); }
	:	'{' (fieldAssignment (',' fieldAssignment)* ','?)? '}' -> ^(EXPRESSION["ObjectCreation"] { $objectCreation::mappings });
  catch [MissingTokenException re] { explainUsage("expected <,> or <}> after a complete field assignment inside of a json object", re); }

literal
@init { paraphrase.push("a literal"); }
@after { paraphrase.pop(); }
	: val='true' -> ^(EXPRESSION["ConstantExpression"] { Boolean.TRUE })
	| val='false' -> ^(EXPRESSION["ConstantExpression"] { Boolean.FALSE })
	| val=DECIMAL -> ^(EXPRESSION["ConstantExpression"] { new BigDecimal($val.text) })
	| val=STRING -> ^(EXPRESSION["ConstantExpression"] { $val.getText() })
  | (val=UINT | val=INTEGER) -> ^(EXPRESSION["ConstantExpression"] { parseInt($val.text) })
  | 'null' -> { ConstantExpression.NULL };

streamIndexAccess
  : op=VAR { getVariable($op) != null }?=>
    '[' path=generalPathExpression ']' { !($path.tree instanceof ConstantExpression) }?
  -> { new StreamIndexExpression(getVariable($op).getStream(), $path.tree) };
	
arrayCreation
@init { paraphrase.push("a json array"); }
@after { paraphrase.pop(); }
	:	 '[' elems+=expression (',' elems+=expression)* ','? ']' -> ^(EXPRESSION["ArrayCreation"] { $elems.toArray(new EvaluationExpression[$elems.size()]) });

/*
 * An operator is either read/write or generic operator.
 * Each operator defines its own variable scope.
 */
operator returns [Operator<?> op=null]
scope { 
  int numInputs;
  Operator<?> result;
}
@init {
  if(state.backtracking == 0) 
	  addScope();
}
@after {
  removeScope();
}:	readOperator { $op = $readOperator.source; }
 |  writeOperator { $op = $writeOperator.sink; }
 |  genericOperator { $op = $genericOperator.op; }; 

// read <format> from <path> options*
readOperator returns [Source source]
@init { 
  ConfObjectInfo<? extends SopremoFileFormat> fileFormatInfo = null;
  SopremoFileFormat fileFormat = null;
  String path = null;
}
	:	'read' ((packageName=ID ':')?format=ID)?
	  'from' (protocol=ID? filePath=STRING | protocol=ID '(' filePath=STRING ')') 
{ 
  path = makeFilePath($protocol, $filePath.text);
  fileFormatInfo = findFormat($packageName.text, format, path);
  fileFormat = fileFormatInfo.newInstance(); 
}
	  confOption[fileFormatInfo, fileFormat]* 
{ $source = new Source(fileFormat, path); } ->;

// write <format> <input> to <path> options*
writeOperator returns [Sink sink]
@init { 
  ConfObjectInfo<? extends SopremoFileFormat> fileFormatInfo = null;
  SopremoFileFormat fileFormat = null;
  String path = null;
}
	:	'write' ((packageName=ID ':')?format=ID)? from=VAR 
	  'to' (protocol=ID? filePath=STRING | protocol=ID '(' filePath=STRING ')')
{ 
  path = makeFilePath($protocol, $filePath.text);
  fileFormatInfo = findFormat($packageName.text, format, path);
  fileFormat = fileFormatInfo.newInstance();
}
    confOption[fileFormatInfo, fileFormat]* 
{ 
	$sink = new Sink(fileFormat, makeFilePath($protocol, path));
  $sink.setInputs(getVariable(from).getStream());
  this.sinks.add($sink);
} ->;

// <name> flags* <input>* options*
// flags - boolean options
// inputs - variables starting with $
// options - key value pairs of property name and value
genericOperator returns [Operator<?> op]
@init { 
  ConfObjectInfo<? extends Operator<?>> operatorInfo;
}	:	(packageName=ID ':')? name=ID { (operatorInfo = findOperatorGreedily($packageName.text, $name)) != null }?=>
{ $operator::result = $op = operatorInfo.newInstance(); } 
((operatorFlag[operatorInfo, $op]*
(('[')=> arrayInput | (VAR)=> input[operatorInfo, $op] ((',')=> ',' input[operatorInfo, $op])*)
confOption[operatorInfo, $op]*)|(confOption[operatorInfo, $op]+)) 
->; 
	
confOption [ConfObjectInfo<?> info, ConfigurableSopremoType object]
@init {
 ConfObjectInfo.ConfObjectPropertyInfo property = null;
} : //{ findOperatorPropertyRelunctantly($genericOperator::operatorInfo, input.LT(1)) != null }?	
  name=ID
	{ property = findPropertyRelunctantly(info, name); }
  expr=contextAwareExpression[null] { property.setValue(object, $expr.tree); } ->;

operatorFlag [ConfObjectInfo<?> info, ConfigurableSopremoType object]
@init {
 ConfObjectInfo.ConfObjectPropertyInfo property = null;
}
  : name=ID  { (property = findPropertyRelunctantly(info, $name)) != null }?
{ if(!property.isFlag())
    throw new QueryParserException(String.format("Property \%s is not a flag", $name.text), name);
  property.setValue(object, true); } ->;

input	[ConfObjectInfo<?> info, Operator<?> object]
@init {
 ConfObjectInfo.ConfObjectIndexedPropertyInfo inputProperty;
}	:	(name=VAR IN)? from=VAR
{ 
  int inputIndex = $operator::numInputs++;
  JsonStreamExpression input = getVariable(from);
  object.setInput(inputIndex, input.getStream());
  
  JsonStreamExpression inputExpression = new JsonStreamExpression(input.getStream(), inputIndex);
  putVariable(name != null ? name : from, inputExpression);
} 
({ (inputProperty = findInputPropertyRelunctantly(info, input.LT(1))) != null }?=> 
  { this.input.consume(); } // consume the initial token
  expr=contextAwareExpression[new InputSelection($operator::numInputs - 1)] { inputProperty.setValue(object, $operator::numInputs-1, $expr.tree); })?
-> ;

arrayInput
  : '[' names+=VAR (',' names+=VAR)? ']' 'in' from=VAR
{ 
  $operator::result.setInput(0, getVariable(from).getStream());
  for(int index = 0; index < $names.size(); index++) {
	  putVariable((Token) $names.get(index), new JsonStreamExpression(null, index)); 
  }
} -> ;

/**
 * Lexer rules
 */	
fragment LOWER_LETTER
	:	'a'..'z';

fragment UPPER_LETTER
	:	'A'..'Z';

fragment DIGIT
	:	'0'..'9';

fragment SIGN:	('+'|'-');

// TYPE  : 'int' | 'decimal' | 'double' | 'string' | 'bool';

JAVAUDF : 'javaudf';

OR  : 'or';

AND  : 'and';

IF  : 'if';

ELSE  : 'else';

NOT : 'not';

IN  : 'in';

FN  : 'fn';

AS  : 'as';

ID	:	(LOWER_LETTER | UPPER_LETTER) (LOWER_LETTER | UPPER_LETTER | DIGIT | '_')*;

VAR	:	'$' ID;

STAR	:	'*';

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;
    
SLASH
    :   '/'
    ;
    
fragment APOSTROPHE
  : '\'';
  
fragment QUOTATION
  : '\"';
    
WS 	:	(' '|'\t'|'\n'|'\r')+ { skip(); };
    
STRING
	:	(QUOTATION (options {greedy=false;} : .)* QUOTATION | APOSTROPHE (options {greedy=false;} : .)* APOSTROPHE)
	{ setText(getText().substring(1, getText().length()-1)); };

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC	:   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT   ;
    
fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;


UINT :	'0'..'9'+;
    
INTEGER :	('+'|'-')? UINT;

DECIMAL
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

