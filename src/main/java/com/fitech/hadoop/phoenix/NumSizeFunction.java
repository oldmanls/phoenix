package com.fitech.hadoop.phoenix;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
/**
 * 
 * Created by co2y on 16/4/5.
 * where (field,">=90","Y") = true
 * field涓篸ouble鍨�
 *
 */
@BuiltInFunction(name = NumSizeFunction.NAME, args = { 
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		@org.apache.phoenix.parse.FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true) })
public class NumSizeFunction extends ScalarFunction{
	public static final String NAME = "NUMSIZE";
	public static final PDataType TYPE = PVarchar.INSTANCE;
	protected String formu;
	protected String yesOrNo;
	
    public NumSizeFunction(){}
	public NumSizeFunction(List<Expression> children){
		super(children);
	}
	
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		String formu = (String) ((LiteralExpression) this.children.get(1)).getValue();
		String yesOrNo = (String) ((LiteralExpression) this.children.get(2)).getValue();
		init(formu, yesOrNo);
	}
	private void init(String formu, String yesOrNo) {
		this.formu = formu;
		this.yesOrNo = yesOrNo;
	}
	
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {		
		Expression formuExp = (Expression) getChildren().get(1);
		if (!formuExp.evaluate(tuple, ptr)) {
			if (this.yesOrNo.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return false;
		}
		formu = ((String) formuExp.getDataType().toObject(ptr, formuExp.getSortOrder()));
		
		Expression yesOrNoExp = (Expression) getChildren().get(2);
		if (!yesOrNoExp.evaluate(tuple, ptr)) {
			return false;
		}
		this.yesOrNo = ((String) yesOrNoExp.getDataType().toObject(ptr, yesOrNoExp.getSortOrder()));
		
		Expression fieldExp = (Expression) getChildren().get(0);
		if (!fieldExp.evaluate(tuple, ptr)) {
			if (this.yesOrNo.equals("Y")) {
				ptr.set(PVarchar.INSTANCE.toBytes("true"));
			}
			return true;
		}
		if ((ptr.getLength() == 0) && (this.yesOrNo.equals("Y"))) {
			ptr.set(PVarchar.INSTANCE.toBytes("true"));
			return true;
		}
		if ((ptr.getLength() == 0) && (this.yesOrNo.equals("N"))) {
			ptr.set(PVarchar.INSTANCE.toBytes("false"));
			return true;
		}
		String field= (String) fieldExp.getDataType().toObject(ptr, fieldExp.getSortOrder());
        Double fieldexp=Double.parseDouble(field);
		
		if (formu.startsWith("<>")) {
            if (fieldexp != Integer.parseInt(formu.substring(2))) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            }
        } else if (formu.startsWith("<=")) {
            if (fieldexp <= Integer.parseInt(formu.substring(2))) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            }
        } else if (formu.startsWith(">=")) {
            if (fieldexp >= Integer.parseInt(formu.substring(2))) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            }
        } else if (formu.startsWith("<")) {
            if (fieldexp< Integer.parseInt(formu.substring(1))) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            }
        } else if (formu.startsWith(">")) {
            if (fieldexp > Integer.parseInt(formu.substring(1))) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            }
        } else if (formu.startsWith("=")) {
            if (fieldexp == Integer.parseInt(formu.substring(1))) {
            	ptr.set(PVarchar.INSTANCE.toBytes("true"));
            }
        }
		ptr.set(PVarchar.INSTANCE.toBytes("false"));
		return true;
	}

	@Override
	public PDataType getDataType() {
		return PVarchar.INSTANCE;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
