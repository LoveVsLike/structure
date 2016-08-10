package com.dream.data;

import java.util.ArrayList;
import java.util.List;

public class LinkedStack<T> implements Stack<T>{

  private List<T> data = new ArrayList<T>();
  private int size;
  
  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public void clean() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int length() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public T pop() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean push(T data) {
    // TODO Auto-generated method stub
    return false;
  }

}
