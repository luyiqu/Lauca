package serializable;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import accessdistribution.SequentialParaDistribution;

public class SequentialParaDistributionAdapter
		implements JsonSerializer<SequentialParaDistribution>, JsonDeserializer<SequentialParaDistribution> {

	@Override
	public SequentialParaDistribution deserialize(JsonElement arg0, Type arg1, JsonDeserializationContext arg2)
			throws JsonParseException {
		JsonObject jsonObject = arg0.getAsJsonObject();
		String type = jsonObject.get("type").getAsString();
		JsonElement element = jsonObject.get("properties");

		try {
			return arg2.deserialize(element, Class.forName(type));
		} catch (ClassNotFoundException cnfe) {
			throw new JsonParseException("Unknown element type: " + type, cnfe);
		}
	}

	@Override
	public JsonElement serialize(SequentialParaDistribution arg0, Type arg1, JsonSerializationContext arg2) {
		JsonObject result = new JsonObject();
		result.add("type", new JsonPrimitive(arg0.getClass().getName()));
		result.add("properties", arg2.serialize(arg0, arg0.getClass()));

		return result;
	}

}
