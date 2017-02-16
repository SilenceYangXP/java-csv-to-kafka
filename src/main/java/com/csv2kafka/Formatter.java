package com.csv2kafka;

import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;

public class Formatter
{

  public JSONObject format(HashMap<String, String> data)
  {
    JSONObject json = new JSONObject();

    for (Map.Entry<String, String> entry : data.entrySet()) {
      json.put(entry.getKey(), entry.getValue());
    }

    return json;
  }

}
