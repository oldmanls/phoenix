package com.fitech.hadoop.phoenix;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;

/**
 * 
 * CHARLENGTH(field,"8-160")
 *
 */
@BuiltInFunction(name = CharLength.NAME, args = { @Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PVarchar.class }) })
public class CharLength extends ScalarFunction {
	public static final String NAME = "CHARLENGTH";

	private String strToFormu = null;

	public CharLength() {}

	public CharLength(List<Expression> children) {
		super(children);
		init();
	}

	private void init() {
		Expression formuExpression = getChildren().get(1);
		if (formuExpression instanceof LiteralExpression) {
			Object strToSearchValue = ((LiteralExpression) formuExpression).getValue();
			if (strToSearchValue != null) {
				this.strToFormu = strToSearchValue.toString();
			}
		}
	}

	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression child = getChildren().get(0);

		if (!child.evaluate(tuple, ptr)) {
			return false;
		}

		if (ptr.getLength() == 0) {
			ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
			return true;
		}
		
		String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());
		
		if(strToFormu.contains("-")){
			int min = Integer.parseInt(strToFormu.split("-")[0]);
            int max = Integer.parseInt(strToFormu.split("-")[1]);
            if (sourceStr.length() >= min && sourceStr.length() <= max) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            	return true;
            }else{
            	ptr.set(PVarchar.INSTANCE.toBytes("false"));
            	return true;
            }
		}
		return false;
	}

	public PDataType getDataType() {
		return PVarchar.INSTANCE;
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		init();
	}

}
