package com.shah.batch.config;

import com.shah.batch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {
   // here we can add all the filter logic before saving into db.
    @Override
    public Customer process(Customer customer) throws Exception {
//        if(!customer.getCountry().equals("In")){
//            return customer;
//        }
        return customer;
    }
}
