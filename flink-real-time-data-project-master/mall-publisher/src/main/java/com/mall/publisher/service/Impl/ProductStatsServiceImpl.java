package com.mall.publisher.service.Impl;

import com.mall.publisher.entity.ProductStats;
import com.mall.publisher.mapper.ProductStatsMapper;
import com.mall.publisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 11:43
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;
    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }
    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        return productStatsMapper.getProductStatsGroupBySpu(date, limit);
    }
    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsGroupByCategory3(date, limit);
    }
    @Override
    public List<ProductStats> getProductStatsByTrademark(int date,int limit) {
        return productStatsMapper.getProductStatsByTrademark(date, limit);
    }

}
