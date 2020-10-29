package com.itheima.report.controller;

import org.junit.Test;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

//表示是controller，并且其中所有的方法都是带有responseBody的。
@RestController
public class TestController {

    @GetMapping("/test")
     public void  test(String json){
         System.out.println(json);
     }
}
