package org.apache.accumulo.server.conf.propstore.v3;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropNameTest {

  Logger log = LoggerFactory.getLogger(PropNameTest.class);

  @Test public void parseTest1() {
    PropName p1 = PropName.parse("x.y.z");

    log.info("scope: {}, namespace: {}, name: {}", p1.getScope(), p1.getNamespace(), p1.getName());

  }
}
