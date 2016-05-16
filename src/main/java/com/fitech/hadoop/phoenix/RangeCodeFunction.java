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
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

@FunctionParseNode.BuiltInFunction(name = "RANGECODE", args = {
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true) })
public class RangeCodeFunction extends ScalarFunction {
	public static final String NAME = "RANGECODE";
	private static final PDataType TYPE = PVarchar.INSTANCE;

	public RangeCodeFunction() {
	}

	public RangeCodeFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	private String ifNUll;
	private String formu1;
	private String formu2;
	private String formu3;
	private String formu4;

	private void init(String ifNUll, String formu1, String formu2, String formu3, String formu4) {
		this.ifNUll = ifNUll;
		this.formu1 = formu1;
		this.formu2 = formu2;
		this.formu3 = formu3;
		this.formu4 = formu4;
	}

	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		String ifNUll = (String) ((LiteralExpression) this.children.get(1)).getValue();
		String formu1 = (String) ((LiteralExpression) this.children.get(2)).getValue();
		String formu2 = (String) ((LiteralExpression) this.children.get(3)).getValue();
		String formu3 = (String) ((LiteralExpression) this.children.get(4)).getValue();
		String formu4 = (String) ((LiteralExpression) this.children.get(5)).getValue();
		init(ifNUll, formu1, formu2, formu3, formu4);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression formu4Exp = (Expression) getChildren().get(5);
		if (!formu4Exp.evaluate(tuple, ptr)) {
			if (this.ifNUll.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return false;
		}
		this.formu4 = ((String) formu4Exp.getDataType().toObject(ptr, formu4Exp.getSortOrder()));
		Expression formu3Exp = (Expression) getChildren().get(4);
		if (!formu3Exp.evaluate(tuple, ptr)) {
			if (this.ifNUll.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return false;
		}
		this.formu3 = ((String) formu3Exp.getDataType().toObject(ptr, formu3Exp.getSortOrder()));
		Expression formu2Exp = (Expression) getChildren().get(3);
		if (!formu2Exp.evaluate(tuple, ptr)) {
			if (this.ifNUll.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return false;
		}
		this.formu2 = ((String) formu2Exp.getDataType().toObject(ptr, formu2Exp.getSortOrder()));
		Expression formu1Exp = (Expression) getChildren().get(2);
		if (!formu1Exp.evaluate(tuple, ptr)) {
			if (this.ifNUll.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return false;
		}
		this.formu1 = ((String) formu1Exp.getDataType().toObject(ptr, formu1Exp.getSortOrder()));
		Expression ifNUllExp = (Expression) getChildren().get(1);
		if (!ifNUllExp.evaluate(tuple, ptr)) {
			return false;
		}
		this.ifNUll = ((String) ifNUllExp.getDataType().toObject(ptr, ifNUllExp.getSortOrder()));
		Expression fieldExp = (Expression) getChildren().get(0);
		if (!fieldExp.evaluate(tuple, ptr)) {
			if (this.ifNUll.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return true;
		}
		if ((ptr.getLength() == 0) && (this.ifNUll.equals("Y"))) {
			ptr.set(PVarchar.INSTANCE.toBytes("true"));
			return true;
		} else if ((ptr.getLength() == 0) && (this.ifNUll.equals("N"))) {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
			return true;
		} else {
			boolean flag = false;
			String fieldStr = (String) fieldExp.getDataType().toObject(ptr, fieldExp.getSortOrder());
			String[] formus = { formu1, formu2, formu3, formu4 };
			for (String formu : formus) {
				if (formu.equals("null")) {
					continue;
				} else if (!formu.contains("[")) {
					if (Integer.parseInt(fieldStr) == Integer.parseInt(formu)) {
						ptr.set(PVarchar.INSTANCE.toBytes("true"));
						return true;
					}
				} else {
					String[] range = formu.split(",");
					int min = Integer.parseInt(range[0].substring(1, range[0].length()));
					int max = Integer.parseInt(range[1].substring(0, range[1].length() - 1));
					if (Integer.parseInt(fieldStr) <= max && Integer.parseInt(fieldStr) >= min) {
						flag = true;
						ptr.set(PVarchar.INSTANCE.toBytes("true"));
						return true;
					}
				}
			}
			if (flag == false) {
				ptr.set(PVarchar.INSTANCE.toBytes("false"));
				return true;
			}
		}
		return false;
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
