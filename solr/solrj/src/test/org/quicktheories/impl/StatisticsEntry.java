package org.quicktheories.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StatisticsEntry  {
  public static final StatisticsEntry NULL = new StatisticsEntry(null, null, 0, 0.0);

  static StatisticsEntry nullFor(List<Object> values) {
    return new StatisticsEntry(values, StatisticsEntry.displayString(values), 0 , 0.0);
  }

  private final List<Object> values;
  private final String name;
  private final int count;
  private final double percentage;

  public StatisticsEntry(List<Object> values, String name, int count, double percentage) {
    this.values = values;
    this.name = name;
    this.count = count;
    this.percentage = percentage;
  }

  StatisticsEntry plus(StatisticsEntry other) {
    int newCount = count + other.count;
    double newPercentage = percentage + other.percentage;
    return new StatisticsEntry(Collections.emptyList(), "<adhoc query>", newCount, newPercentage);
  }

  public String name() {
    return name;
  }


  public int count() {
    return count;
  }


  public double percentage() {
    return percentage;
  }


  public List<Object> values() {
    return values;
  }


  public String toString() {
    return String.format("%s (%s, %s%%): %s", name, count, percentage, StatisticsEntry.displayString(values));
  }

  public static String displayString(Object object) {
    if (object == null)
      return "null";
    if (object instanceof Class) {
      return ((Class) object).getName();
    }
    if (object instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<Object> collection = (Collection<Object>) object;
      String elements = collection.stream().map(StatisticsEntry::displayString).collect(Collectors.joining(", "));
      return String.format("[%s]", elements);
    }
    if (object.getClass().isArray()) {
      if (object.getClass().getComponentType().isPrimitive()) {
        return nullSafeToString(object);
      }
      Object[] array = (Object[]) object;
      String elements = Arrays.stream(array).map(StatisticsEntry::displayString).collect(Collectors.joining(", "));
      return String.format("%s{%s}", object.getClass().getSimpleName(), elements);
    }
    if (String.class.isAssignableFrom(object.getClass())) {
      return String.format("\"%s\"", replaceUnrepresentableCharacters(object.toString()));
    }
    return replaceUnrepresentableCharacters(object.toString());
  }


  private static String nullSafeToString(Object obj) {
    if (obj == null) {
      return "null";
    }

    try {
      if (obj.getClass().isArray()) {
        if (obj.getClass().getComponentType().isPrimitive()) {
          if (obj instanceof boolean[]) {
            return Arrays.toString((boolean[]) obj);
          }
          if (obj instanceof char[]) {
            return Arrays.toString((char[]) obj);
          }
          if (obj instanceof short[]) {
            return Arrays.toString((short[]) obj);
          }
          if (obj instanceof byte[]) {
            return Arrays.toString((byte[]) obj);
          }
          if (obj instanceof int[]) {
            return Arrays.toString((int[]) obj);
          }
          if (obj instanceof long[]) {
            return Arrays.toString((long[]) obj);
          }
          if (obj instanceof float[]) {
            return Arrays.toString((float[]) obj);
          }
          if (obj instanceof double[]) {
            return Arrays.toString((double[]) obj);
          }
        }
        return Arrays.deepToString((Object[]) obj);
      }

      // else
      return obj.toString();
    }
    catch (Throwable throwable) {
    //  JqwikExceptionSupport.rethrowIfBlacklisted(throwable);
      return defaultToString(obj);
    }
  }

  private static String defaultToString(Object obj) {
    if (obj == null) {
      return "null";
    }

    return obj.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(obj));
  }

  private static String replaceUnrepresentableCharacters(String aString) {
    return aString.replace('\u0000', '\ufffd');
  }

}