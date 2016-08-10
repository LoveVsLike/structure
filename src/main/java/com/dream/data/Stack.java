package com.dream.data;

public interface Stack<T> {
  /**
   * 判断一个栈是否为空
   */
  public boolean isEmpty();
  /**
   * 清空一个栈
   */
  public void clean();
  
  /**
   * 计算出栈的长度
   */
  public int length();
  
  /**
   * 出栈
   */
  public T pop();
  
  /**
   * 入栈
   */
  public boolean push(T data);
  
}
