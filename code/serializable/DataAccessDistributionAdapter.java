package serializable;

import java.lang.reflect.Type;
import java.math.BigDecimal;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import accessdistribution.ContinuousParaDistribution;
import accessdistribution.DataAccessDistribution;

public class DataAccessDistributionAdapter
		implements JsonSerializer<DataAccessDistribution>, JsonDeserializer<DataAccessDistribution> {

	@Override
	public DataAccessDistribution deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2)
			throws JsonParseException {
		JsonObject jsonObject = arg0.getAsJsonObject();
		String type = jsonObject.get("type").getAsString();
		JsonElement element = jsonObject.get("properties");
		
		if(type.equals("accessdistribution.ContinuousParaDistribution<java.lang.Long>")) {
			return arg2.deserialize(element, new TypeToken<ContinuousParaDistribution<Long>>(){}.getType());
		}else if(type.equals("accessdistribution.ContinuousParaDistribution<java.lang.Double>")) {
			return arg2.deserialize(element, new TypeToken<ContinuousParaDistribution<Double>>(){}.getType());
		}else if(type.equals("accessdistribution.ContinuousParaDistribution<java.math.BigDecimal>")) {
			return arg2.deserialize(element, new TypeToken<ContinuousParaDistribution<BigDecimal>>(){}.getType());
		}

		try {
			// 指定包名+类名
			return arg2.deserialize(element, Class.forName(type));
		} catch (ClassNotFoundException cnfe) {
			throw new JsonParseException("Unknown element type: " + type, cnfe);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public JsonElement serialize(DataAccessDistribution arg0, Type arg1, JsonSerializationContext arg2) {
		JsonObject result = new JsonObject();
		if(arg0 instanceof ContinuousParaDistribution) {
			if(((ContinuousParaDistribution)arg0).getMinValue() instanceof Long) {
				result.add("type", new JsonPrimitive(new TypeToken<ContinuousParaDistribution<Long>>(){}.getType().getTypeName()));
			}else if(((ContinuousParaDistribution)arg0).getMinValue() instanceof Double) {
				result.add("type", new JsonPrimitive(new TypeToken<ContinuousParaDistribution<Double>>(){}.getType().getTypeName()));
			}else if(((ContinuousParaDistribution)arg0).getMinValue() instanceof BigDecimal) {
				result.add("type", new JsonPrimitive(new TypeToken<ContinuousParaDistribution<BigDecimal>>(){}.getType().getTypeName()));
				
			}
		}else {
			result.add("type", new JsonPrimitive(arg0.getClass().getName()));
		}
		result.add("properties", arg2.serialize(arg0, arg0.getClass()));
		return result;
	}

}
