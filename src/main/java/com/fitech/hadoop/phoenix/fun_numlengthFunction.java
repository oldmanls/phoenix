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
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
@FunctionParseNode.BuiltInFunction(name = "NUMLENGTH", args = {
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PDouble.class}),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PInteger.class },isConstant = true),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PInteger.class },isConstant = true) })
public class fun_numlengthFunction extends ScalarFunction{
	public static final String NAME="NUMLENGTH";
	private static final PDataType TYPE = PVarchar.INSTANCE;
	public fun_numlengthFunction(){}
	public fun_numlengthFunction(List<Expression> children) throws SQLException {
		super(children);
	}
	private Integer ilength;
	private Integer dlength;
	private void init(Integer ilength,Integer dlength) {
		this.ilength = ilength;
		this.dlength = dlength;
	}
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		Integer ilength = (Integer) ((LiteralExpression) this.children.get(1)).getValue();
		Integer dlength =  (Integer) ((LiteralExpression) this.children.get(2)).getValue();
		init(ilength,dlength);
	}
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression ilengthExp = (Expression) getChildren().get(2);
		if (!ilengthExp.evaluate(tuple, ptr)) {
			return false;
		}
		this.ilength = (Integer) ( ilengthExp.getDataType().toObject(ptr, ilengthExp.getSortOrder()));
		Expression dlengthExp = (Expression) getChildren().get(1);
		if (!dlengthExp.evaluate(tuple, ptr)) {
			return false;
		}
		this.dlength = (Integer) ( dlengthExp.getDataType().toObject(ptr, dlengthExp.getSortOrder()));
		Expression fieldExp = (Expression) getChildren().get(0);
		if (!fieldExp.evaluate(tuple, ptr)) {
			return true;
		}
		if(ptr.getLength() == 0){
			return true;
		}else{
			Double fieldStr = (Double) fieldExp.getDataType().toObject(ptr, fieldExp.getSortOrder());
			String str = fieldStr+"";
			String[] values = str.split("\\.");
	        if (values[0].length() <= (int)(ilength)) {
	            if (values.length == 1) {
	            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
	            	return true;
	            } else {
	                if (values[1].length() <= (int)(dlength)) {
	                	ptr.set(PVarchar.INSTANCE.toBytes("true"));
		            	return true;
	                } else {
	                	ptr.set(PVarchar.INSTANCE.toBytes("false"));
	                	return true;
	                }
	            }
	        } else {
	        	ptr.set(PVarchar.INSTANCE.toBytes("false"));
            	return true;
	        }
		}
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
