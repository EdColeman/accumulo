package org.apache.accumulo.server.conf.propstore.v3;

import org.apache.accumulo.core.data.TableId;

import java.util.Map;

public interface PropStore {

  String get(TableId tableId, String propName);
  String set(TableId tableId, String propName, String propValue);
  Map<String,String> getAll(TableId tableId);

}
