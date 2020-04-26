package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import java.util.Objects;

public class Person {

  private String name;
  private Integer age;

  public Person() {
  }

  public Person(String name, Integer age) {
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public Integer getAge() {
    return age;
  }

  @Override
  public String toString() {
    return "Person, name: " + name + ", age: " + age;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Person person = (Person) o;
    return Objects.equals(getAge(), person.getAge()) &&
        Objects.equals(getName(), person.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getAge());
  }
}
