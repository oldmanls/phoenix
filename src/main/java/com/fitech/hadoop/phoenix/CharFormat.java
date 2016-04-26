package com.fitech.hadoop.phoenix;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * 
 * CHARFORMAT(field)='true'
 *
 */
@BuiltInFunction(name = CharFormat.NAME, 
       args = { @Argument(allowedTypes = { PVarchar.class }) })
public class CharFormat extends ScalarFunction {
	public static final String NAME = "CHARFORMAT";

	public static final PDataType TYPE = PVarchar.INSTANCE;

	public CharFormat() {}

	public CharFormat(List<Expression> children) {
		super(children);
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression fieldExp = (Expression) getChildren().get(0);

		if (!fieldExp.evaluate(tuple, ptr)) {
			return false;
		}
		String field = (String) PVarchar.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());
		
		Pattern p = Pattern.compile("[0-9A-Z]");
	    Matcher m = p.matcher(field);
	     if(m.find()) {
	    	 System.out.println("f1:"+field);
	        ptr.set(PVarchar.INSTANCE.toBytes("true"));
	     } else {
	    	 System.out.println("f1:"+field);
	        ptr.set(PVarchar.INSTANCE.toBytes("false"));
	     }
		  return true;
	}

	public PDataType getDataType() {
		return PVarchar.INSTANCE;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
