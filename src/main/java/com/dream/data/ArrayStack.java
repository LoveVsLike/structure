package com.dream.data;

public class ArrayStack<T> implements Stack<T> {

  private Object[] data = new Object[10];
  private int size = 0;

  @Override
  public boolean isEmpty() {
    if (size > 0) {
      return true;
    }
    return false;
  }

  @Override
  public void clean() {
    for (int i = 0; i < size; i++) {
      data[i] = null;
    }
    size = 0;
  }

  @Override
  public int length() {
    return size;
  }

  @Override
  // 先进后出
  public T pop() {
    if (size > 0) {
      return (T) data[--size];
    }
    return null;
  }

  @Override
  public boolean push(Object datas) {
    if (size >= data.length) {
      // 扩容
      resize();
    }
    data[size++] = datas;
    return true;
  }

  //移动数据
  private void resize() {
    Object[] tmp = new Object[size + 5];
    for(int i = 0; i < size; i++){
      tmp[i] = data[i];
      data[i] = null;
    }
    data = tmp;
  }

}
