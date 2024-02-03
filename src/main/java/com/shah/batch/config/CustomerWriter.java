package com.shah.batch.config;

import com.shah.batch.entity.Customer;
import com.shah.batch.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomerWriter implements ItemWriter<Customer> {
    private final CustomerRepository  customerRepository;
    @Override
    public void write(Chunk<? extends Customer> chunk) throws Exception {
        log.info("Thread Name : -{}",Thread.currentThread().getName());
        customerRepository.saveAll(chunk);
    }
}
