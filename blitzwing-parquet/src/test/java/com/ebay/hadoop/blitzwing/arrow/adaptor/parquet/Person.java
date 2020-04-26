package com.ebay.hadoop.blitzwing.arrow.adaptor.parquet;

import java.util.Objects;

public class Person {

  private String name;
  private Integer age;
  private Long hairCount;


  public Person(String name, Integer age, Long hairCount) {
    this.name = name;
    this.age = age;
    this.hairCount = hairCount;
  }

  public String getName() {
    return name;
  }

  public Integer getAge() {
    return age;
  }

  public Long getHairCount() {
    return hairCount;
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
        Objects.equals(getName(), person.getName()) &&
        Objects.equals(getHairCount(), person.getHairCount());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getAge(), getHairCount());
  }
}
