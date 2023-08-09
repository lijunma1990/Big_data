package com.mall.publisher.service.Impl;

import com.mall.publisher.entity.ProvinceStats;
import com.mall.publisher.mapper.ProvinceStatsMapper;
import com.mall.publisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:09
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;
    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
