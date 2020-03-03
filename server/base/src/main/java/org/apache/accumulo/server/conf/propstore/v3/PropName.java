package org.apache.accumulo.server.conf.propstore.v3;

import java.util.Objects;
import java.util.StringJoiner;

public class PropName {

  public enum PropScope {
    DEFAULT, TABLE, NAMESPACE, SYSTEM
  }

  private final PropScope scope;
  private final String namespace;
  private final String name;

  private PropName(final PropScope scope, final String namespace, final String name) {
    this.scope = scope;
    this.namespace = namespace;
    this.name = name;
  }

  public static PropName defaultProp(final String name) {
    return new PropName(PropScope.DEFAULT, "", name);
  }

  public static PropName systemProp(final String name) {
    return new PropName(PropScope.SYSTEM, "", name);
  }

  public static PropName namespaceProp(final String name) {
    return new PropName(PropScope.NAMESPACE, "", name);
  }

  public static PropName propName(final String name) {
    return new PropName(PropScope.TABLE, "", name);
  }

  public static PropName parse(final String name) {
    NamePair pair = NamePair.parse(name);
    return new PropName(PropScope.TABLE, pair.getNameSpace(), pair.getName());
  }

  public PropScope getScope() {
    return scope;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  private static class NamePair {

    private final String nameSpace;
    private final String name;

    private NamePair(final String nameSpace, final String name) {
      this.nameSpace = nameSpace;
      this.name = name;
    }

    public String getNameSpace() {
      return nameSpace;
    }

    public String getName() {
      return name;
    }

    public static NamePair parse(final String value) {
      int firstDot = value.indexOf('.');
      if (firstDot == -1) {
        return new NamePair("", value);
      }
      return new NamePair(value.substring(0, firstDot), value.substring(firstDot+1));
    }
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PropName propName = (PropName) o;
    return scope == propName.scope && namespace.equals(propName.namespace) && name
        .equals(propName.name);
  }

  @Override public int hashCode() {
    return Objects.hash(scope, namespace, name);
  }

  @Override public String toString() {
    return new StringJoiner(", ", PropName.class.getSimpleName() + "[", "]").add("scope=" + scope)
        .add("namespace='" + namespace + "'").add("name='" + name + "'").toString();
  }
}
