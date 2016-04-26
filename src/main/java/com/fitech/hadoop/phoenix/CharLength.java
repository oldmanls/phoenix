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
 * Author LS 2015/04/21 CHARLENGTH(field,"8-160","Y or N")
 */
@BuiltInFunction(name = CharLength.NAME, args = { @Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		@Argument(allowedTypes = { PVarchar.class }, isConstant = true) })
public class CharLength extends ScalarFunction {
	public static final String NAME = "CHARLENGTH";

	private static final PDataType TYPE = PVarchar.INSTANCE;

	public CharLength() {
	}

	public CharLength(List<Expression> children) throws SQLException {
		super(children);
	}

	// 校验
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!getFormuExpression().evaluate(tuple, ptr)) {
			return false;
		}

		String formu = (String) getFormuExpression().getDataType().toObject(ptr, getFormuExpression().getSortOrder());

		if (!getYesOrNoExpression().evaluate(tuple, ptr)) {
			return false;
		}

		String yesOrNo = (String) getYesOrNoExpression().getDataType().toObject(ptr,
				getYesOrNoExpression().getSortOrder());

		if (!getSourceStrExpression().evaluate(tuple, ptr)) {
			return false;
		}

		String sourceStr = (String) getSourceStrExpression().getDataType().toObject(ptr,
				getSourceStrExpression().getSortOrder());

		// 得到字段值为空
		if (ptr.getLength() == 0) {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
			// 如果不允许为空
			if (yesOrNo.equals("N")) {
				ptr.set(PVarchar.INSTANCE.toBytes("false"));
				// 如果允许为空
			} else {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			// 得到的字段值不为空
		} else {
			ptr.set(PVarchar.INSTANCE.toBytes("true"));
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

	// 获取是否可为空标志,"Y"=可谓空，"N"=不可为空
	private Expression getYesOrNoExpression() {
		return children.get(2);
	}

	public PDataType getDataType() {
		return TYPE;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
