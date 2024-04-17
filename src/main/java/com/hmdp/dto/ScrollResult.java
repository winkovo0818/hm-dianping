package com.hmdp.dto;

import lombok.Data;

import java.util.List;

/**
 * 滚动查询 封装
 */
@Data
public class ScrollResult {
    private List<?> list;
    private Long minTime;
    private Integer offset;
}
