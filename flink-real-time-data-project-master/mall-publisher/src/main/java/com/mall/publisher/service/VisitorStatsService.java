package com.mall.publisher.service;

import com.mall.publisher.entity.VisitorStats;
import com.mall.publisher.mapper.VisitorStatsMapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:15
 */
public interface VisitorStatsService {

    public List<VisitorStats> getVisitorStatsByNewFlag(int date);
    public List<VisitorStats> getVisitorStatsByHour(int date);
    public Long getPv(int date);
    public Long getUv(int date);
}
