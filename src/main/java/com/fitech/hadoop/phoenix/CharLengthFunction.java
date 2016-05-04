package com.fitech.hadoop.phoenix;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * Author LS 2015/04/21 CHARLENGTH(field,"8-160")
 */
@BuiltInFunction(name = CharLengthFunction.NAME, args = { @Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PVarchar.class }, isConstant = true) })
public class CharLengthFunction extends ScalarFunction {
	public static final String NAME = "CHARLENGTH";

	private static final PDataType TYPE = PVarchar.INSTANCE;

	public CharLengthFunction() {
	}

	public CharLengthFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	// 校验
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!getFormuExpression().evaluate(tuple, ptr)) {
			return false;
		}

		String formu = (String) getFormuExpression().getDataType().toObject(ptr, getFormuExpression().getSortOrder());

		if (!getSourceStrExpression().evaluate(tuple, ptr)) {
			return false;
		}

		String sourceStr = (String) getSourceStrExpression().getDataType().toObject(ptr,
				getSourceStrExpression().getSortOrder());

		if (formu.contains("-")) {
			int min = Integer.parseInt(formu.split("-")[0]);
			int max = Integer.parseInt(formu.split("-")[1]);
			if (sourceStr.length() >= min && sourceStr.length() <= max) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			} else {
				ptr.set(PVarchar.INSTANCE.toBytes("false"));
			}
		} else {
			int max1 = Integer.parseInt(formu);
			if (sourceStr.length() <= max1) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			} else {
				ptr.set(PVarchar.INSTANCE.toBytes("false"));
			}
		}
		return true;
	}

	// 获取字段值
	private Expression getSourceStrExpression() {
		return children.get(0);
	}

	// 获取公式
	private Expression getFormuExpression() {
		return children.get(1);
	}

	public PDataType getDataType() {
		return TYPE;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
