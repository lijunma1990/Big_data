package com.mall.publisher.service.Impl;

import com.mall.publisher.entity.VisitorStats;
import com.mall.publisher.mapper.VisitorStatsMapper;
import com.mall.publisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:16
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }
    @Override
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return visitorStatsMapper.selectVisitorStatsByHour(date);
    }
    @Override
    public Long getPv(int date) {
        return visitorStatsMapper.selectPv(date);
    }
    @Override
    public Long getUv(int date) {
        return visitorStatsMapper.selectUv(date);
    }
}
