package com.fitech.hadoop.phoenix;

import java.io.DataInput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

@FunctionParseNode.BuiltInFunction(name = CharContainFunction.NAME, args = { @Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PVarchar.class }) })
public class CharContainFunction extends ScalarFunction {
	public static final String NAME = "CHARCONTAIN";
	private static final PDataType TYPE = PVarchar.INSTANCE;
	protected String formu;

	public CharContainFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	public String getName() {
		return "CHARCONTAIN";
	}

	private void init(String formu) {
		this.formu = formu;
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable immutableBytesWritable) {
		Expression formuExp = (Expression) getChildren().get(1);
		if (!formuExp.evaluate(tuple, immutableBytesWritable)) {
			return false;
		}
		this.formu = ((String) formuExp.getDataType().toObject(immutableBytesWritable, formuExp.getSortOrder()));

		Expression fieldExp = (Expression) getChildren().get(0);
		if (!fieldExp.evaluate(tuple, immutableBytesWritable)) {
			return false;
		}

		String fieldStr = (String) fieldExp.getDataType().toObject(immutableBytesWritable, fieldExp.getSortOrder());

		String[] values = this.formu.split("\\|");
		for (String value : values) {
			if (fieldStr.contains(value)) {
				immutableBytesWritable.set(PVarchar.INSTANCE.toBytes("true"));
				return true;
			}
		}
		immutableBytesWritable.set(PVarchar.INSTANCE.toBytes("false"));
		return true;
	}

	public PDataType getDataType() {
		return TYPE;
	}

	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		String formu = (String) ((LiteralExpression) this.children.get(1)).getValue();
		init(formu);
	}
}
