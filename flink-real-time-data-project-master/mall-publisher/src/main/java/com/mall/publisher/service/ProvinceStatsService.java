package com.mall.publisher.service;

import com.mall.publisher.entity.ProvinceStats;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:09
 */
public interface ProvinceStatsService {
    public List<ProvinceStats> getProvinceStats(int date);
}
