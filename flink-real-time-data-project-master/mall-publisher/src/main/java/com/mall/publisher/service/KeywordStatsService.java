package com.mall.publisher.service;

import com.mall.publisher.entity.KeywordStats;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:22
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
