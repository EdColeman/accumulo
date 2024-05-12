/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.accumulo.annotations;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

@SupportedAnnotationTypes("org.apache.accumulo.annotations.MetricsDocProperty")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class MetricsDocProcessor extends AbstractProcessor {

  private final AtomicInteger index = new AtomicInteger(0);

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    try (PrintWriter pw =
        new PrintWriter(new FileOutputStream("/tmp/ann_" + index.incrementAndGet() + ".txt"))) {

      pw.println("ANNOTATION:" + annotations.toString() + ", roundEnv: " + roundEnv);

      for (TypeElement annotation : annotations) {
        pw.println("ANNOTATION:" + annotation);
        Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);

        for (Element element : annotatedElements) {
          pw.println("ANNOTATED ELEMENTS:" + element);
          var mods = element.getModifiers();
          mods.forEach(m -> pw.println("  " + m.toString()));

          var a = element.getAnnotation(MetricsDocProperty.class);
          pw.println("  " + a.name());
          pw.println("  " + a.description());

          var v = a.versions();
          Arrays.stream(v).forEach(ver -> {
            pw.println("  " + ver.version());
            pw.println("  " + ver.prevName());
          });
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return true;
  }
}
