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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;

@SupportedAnnotationTypes("org.apache.accumulo.annotations.MetricsDocProperty")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
@SupportedOptions({"output.dir"})
public class MetricsDocProcessor extends AbstractProcessor {

  private PrintWriter pw;

  private Elements elementUtils;

  public MetricsDocProcessor() {
    super();
  }

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    this.elementUtils = processingEnv.getElementUtils();

    String dir = super.processingEnv.getOptions().getOrDefault("output.dir",
        System.getProperty("java.io.tmpdir"));
    if (!Files.exists(Path.of(dir))) {
      try {
        Files.createDirectory(Path.of(dir));
      } catch (IOException ex) {
        throw new IllegalStateException(ex);
      }
    }

    String filename = dir + "/accumulo_metrics.md";

    try {
      FileWriter fw = new FileWriter(filename, UTF_8, true);
      BufferedWriter bw = new BufferedWriter(fw);
      pw = new PrintWriter(bw);
    } catch (IOException ex) {
      System.err.println(Arrays.toString(ex.getStackTrace()));
    }
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    Set<String> result = super.getSupportedAnnotationTypes();
    result = new TreeSet<>(result);
    result.add(MetricsDocProperty.class.getCanonicalName());
    return result;
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

    System.out.println("MetricsDocProcessor: processing " + annotations.size() + " annotations");

    for (TypeElement annotation : annotations) {
      pw.println("ANNOTATION:" + annotation);
      Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);

      for (Element element : annotatedElements) {

        Element enclosing = element;
        while (enclosing.getKind() != ElementKind.PACKAGE) {
          enclosing = enclosing.getEnclosingElement();
        }

        PackageElement packageElement = (PackageElement) enclosing;
        pw.println("Package: " + packageElement);

        var mods = element.getModifiers();
        mods.forEach(m -> pw.println("  " + element.getSimpleName() + " -> " + m.toString()));

        printRecord(pw, element);

      }
    }
    pw.flush();
    return false;
  }

  private void printRecord(PrintWriter pw, Element element) {
    var a = element.getAnnotation(MetricsDocProperty.class);

    pw.println("  N:" + a.name());
    pw.println("  D: " + a.description());

    var v = a.versions();
    Arrays.stream(v).forEach(ver -> {
      pw.println("  V:" + ver.version());
      pw.println("  P:" + ver.prevName());
    });
  }
}
