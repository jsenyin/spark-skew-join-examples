package org.apache.spark.sparkLearning.orderexmaple;

import java.io.Serializable;

/**
 * 简单订单
 * @author liangming.deng
 *
 */
public class Order implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//订单名称
	private String name;
	//订单价格
    private Float price;
    

	public Order() {
		super();
	}
    
	public Order(String name, Float price) {
		super();
		this.name = name;
		this.price = price;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Float getPrice() {
		return price;
	}
	public void setPrice(Float price) {
		this.price = price;
	}
	@Override
	public String toString() {
		return "Order [name=" + name + ", price=" + price + "]";
	}
    
}
