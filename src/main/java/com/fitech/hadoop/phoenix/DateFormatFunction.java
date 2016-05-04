package com.fitech.hadoop.phoenix;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
 * 
 * DATEFORMAT(field,"yyyy-MM-dd")
 *
 */
@BuiltInFunction(name = DateFormatFunction.NAME, args = {
		@Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PVarchar.class }) })
public class DateFormatFunction extends ScalarFunction {
	public static final String NAME = "DATEFORMAT";
	public static final PDataType TYPE = PVarchar.INSTANCE;

	public DateFormatFunction() {
	}

	public DateFormatFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression fieldExp = (Expression) getChildren().get(0);
		Expression formuExp = (Expression) getChildren().get(1);
		if (!fieldExp.evaluate(tuple, ptr)) {
			return false;
		}
		String field = (String) fieldExp.getDataType().toObject(ptr, fieldExp.getSortOrder());
		
		if (!formuExp.evaluate(tuple, ptr)) {
			return false;
		}
		String formu = (String) formuExp.getDataType().toObject(ptr, formuExp.getSortOrder());

		SimpleDateFormat format = new SimpleDateFormat(formu);
		try {
			format.setLenient(false);
			format.parse(field);
			ptr.set(PVarchar.INSTANCE.toBytes("true"));
		} catch (ParseException e) {
			System.out.println("f1:"+field+",f2:"+formu);
			e.printStackTrace();
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
