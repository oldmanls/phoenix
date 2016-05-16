package com.fitech.hadoop.phoenix;

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

@FunctionParseNode.BuiltInFunction(name = "IDCODE", args = {
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) })
public class IdCodeFunction extends ScalarFunction {
	public static final String NAME = "IDCODE";
	private static final PDataType TYPE = PVarchar.INSTANCE;

	public IdCodeFunction() {
	}

	public IdCodeFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!getIDCodeExpression().evaluate(tuple, ptr)) {
			return false;
		}
		;
		String IDCode = (String) getIDCodeExpression().getDataType().toObject(ptr,
				getIDCodeExpression().getSortOrder());
		String formu = "(^[0-9]{17}[0-9|x]$)|(^[0-9]{15}$)";
		Pattern p = Pattern.compile(formu);
		Matcher m = p.matcher(IDCode);
		if (m.find()) {
			ptr.set(PVarchar.INSTANCE.toBytes("true"));
			return true;
		} else {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
			return true;
		}
	}

	private Expression getIDCodeExpression() {
		return children.get(0);
	}

	@Override
	public PDataType getDataType() {
		return TYPE;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
