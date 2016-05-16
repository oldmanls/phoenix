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

/**
 * bankcode("1","2")
 * 
 * @author Administrator
 *
 */
@FunctionParseNode.BuiltInFunction(name = "BANKCODE", args = {
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) })
public class BankCodeFunction extends ScalarFunction {
	public static final String NAME = "BANKCODE";
	private static final PDataType TYPE = PVarchar.INSTANCE;

	public BankCodeFunction() {
	}

	public BankCodeFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!getBankCodeExpression().evaluate(tuple, ptr)) {
			return false;
		}

		String bankCode = (String) getBankCodeExpression().getDataType().toObject(ptr,
				getBankCodeExpression().getSortOrder());
		if (bankCode.length() != 15) {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
			return true;
		}
		String formu = "([A-Z]{1})([0-9]{4})([A-Z]{1})([1-3]{1})([0-9]{8})";
		Pattern p = Pattern.compile(formu);
		Matcher m = p.matcher(bankCode);
		if (m.find()) {
			ptr.set(PVarchar.INSTANCE.toBytes("true"));
			return true;
		} else {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
			return true;
		}
	}

	private Expression getBankCodeExpression() {
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
