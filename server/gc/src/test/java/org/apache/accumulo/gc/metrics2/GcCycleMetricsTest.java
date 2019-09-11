/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.gc.metrics2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class GcCycleMetricsTest {

  @Test
  public void empty() {
    GcCycleMetrics v = new GcCycleMetrics();

    assertEquals(0, v.getStarted());
    assertEquals(0, v.getFinished());
    assertEquals(0, v.getCandidates());
    assertEquals(0, v.getInUse());
    assertEquals(0, v.getDeleted());
    assertEquals(0, v.getErrors());
    assertFalse(v.hasError());
    assertEquals("", v.getErrMsg());
  }

  @Test
  public void sets() {
    GcCycleMetrics v = new GcCycleMetrics();

    v.markStarted();
    v.markFinished();
    v.incrementDeleted(17);
    v.incrementDeleted();

    v.incrementErrors(27);
    v.incrementErrors();

    v.incrementCandidates(37);
    v.incrementCandidates();

    v.incrementInUse(47);
    v.incrementInUse();

    v.sawCollectionError("testing");

    assertEquals(18, v.getDeleted());
    assertEquals(28, v.getErrors());
    assertEquals(38, v.getCandidates());
    assertEquals(48, v.getInUse());

    assertTrue(v.hasError());
    assertEquals("testing", v.getErrMsg());
  }
}
