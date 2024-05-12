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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

@SupportedAnnotationTypes("org.apache.accumulo.annotations.MetricsDocProperty")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class MetricsDocProcessor extends AbstractProcessor {

  private static final SecureRandom rand = new SecureRandom();

  private PrintWriter pw;

  public MetricsDocProcessor() {
    super();

    String filename = "/tmp/ann_" + String.format("%04x", rand.nextInt() & 0xffff) + ".txt";

    System.out.println("MetricsDocProcessor::ctor filer: " + filename);

    try {
      FileWriter fw = new FileWriter(filename, UTF_8, true);
      BufferedWriter bw = new BufferedWriter(fw);
      pw = new PrintWriter(bw);
    } catch (IOException ex) {
      System.err.println(Arrays.toString(ex.getStackTrace()));
    }
  }

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    System.out.println("MetricsDocProcessor::init filer: " + processingEnv.getFiler());
    System.out.println("MetricsDocProcessor::init messager: " + processingEnv.getMessager());
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    Set<String> annotations = new HashSet<>();
    annotations.add(MetricsDocProperty.class.getCanonicalName());
    return annotations;
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    System.out.println("MetricsDocProcessor: processing " + annotations.size() + " annotations");

    pw.println("process():" + annotations + ", roundEnv: " + roundEnv);

    for (TypeElement annotation : annotations) {
      pw.println("ANNOTATION:" + annotation);
      Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);

      for (Element element : annotatedElements) {
        pw.println("ANNOTATED ELEMENTS:" + element);
        var mods = element.getModifiers();
        mods.forEach(m -> pw.println("  " + m.toString()));

        printRecord(pw, element);

      }
    }
    pw.flush();
    return false;
  }

  private void printRecord(PrintWriter pw, Element element) {

    System.out.println("ANNOTATION - printRecord:" + element);

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
