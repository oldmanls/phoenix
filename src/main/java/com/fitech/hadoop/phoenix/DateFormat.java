package com.fitech.hadoop.phoenix;

import java.io.DataInput;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * 
 * DATEFORMAT(field,"yyyy-MM-dd")
 *
 */
@BuiltInFunction(name = DateFormat.NAME, args = {
		@Argument(allowedTypes = { PVarchar.class }),
		@Argument(allowedTypes = { PVarchar.class }) })
public class DateFormat extends ScalarFunction {
	public static final String NAME = "DATEFORMAT";
	protected String formu;
	
	public DateFormat(){}
	
	public DateFormat(List<Expression> children) throws SQLException {
	        super(children);
	}
	
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		String formu = (String) ((LiteralExpression) this.children.get(1)).getValue();
		init(formu);
	}
	
	 private void init(String formu) {
			this.formu = formu;
	}

	
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression fieldExp = (Expression) getChildren().get(0);	
		Expression formuExp = (Expression) getChildren().get(1);
	    if (!fieldExp.evaluate(tuple, ptr)) {
	         return false;
	    }
	    if(!fieldExp.evaluate(tuple, ptr)){
	    	return false;
	    }
       String field=(String)fieldExp.toString();
        SimpleDateFormat format = new SimpleDateFormat(formu);
        try {
			ptr.set(PVarchar.INSTANCE.toBytes(format.parse(field)));
			return true;
		} catch (ParseException e) {
			e.printStackTrace();
		}
        return true;

	}

	@Override
	public PDataType getDataType() {
	    return PVarchar.INSTANCE;
	}

	@Override
	public String getName() {
		return null;
	}
  


}
