package com.fitech.hadoop.phoenix;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;

@FunctionParseNode.BuiltInFunction(name = "NUMLENGTH", args = { @Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PInteger.class }), @Argument(allowedTypes = { PInteger.class }) })
public class NumLengthFunction extends ScalarFunction {
	public static final String NAME = "NUMLENGTH";
	private static final PDataType TYPE = PVarchar.INSTANCE;
	private Integer ilength;
	private Integer dlength;

	public NumLengthFunction() {
	}

	public NumLengthFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression ilengthExp = (Expression) getChildren().get(1);
		if (!ilengthExp.evaluate(tuple, ptr)) {
			return false;
		}
		this.ilength = (Integer) (ilengthExp.getDataType().toObject(ptr, ilengthExp.getSortOrder()));

		Expression dlengthExp = (Expression) getChildren().get(2);
		if (!dlengthExp.evaluate(tuple, ptr)) {
			return false;
		}
		this.dlength = (Integer) (dlengthExp.getDataType().toObject(ptr, dlengthExp.getSortOrder()));

		Expression fieldExp = (Expression) getChildren().get(0);
		if (!fieldExp.evaluate(tuple, ptr)) {
			return true;
		}
		String fieldStr = (String) fieldExp.getDataType().toObject(ptr, fieldExp.getSortOrder());

		String[] values = fieldStr.split("\\.");
		if (values[0].length() <= ilength) {
			if (values.length == 1) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			} else {
				if (values[1].length() <= dlength) {
					ptr.set(PVarchar.INSTANCE.toBytes("true"));
				} else {
					ptr.set(PVarchar.INSTANCE.toBytes("false"));
				}
			}
		} else {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
		}
		return true;
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
