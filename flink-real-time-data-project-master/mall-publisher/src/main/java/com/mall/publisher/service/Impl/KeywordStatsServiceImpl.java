package com.mall.publisher.service.Impl;

import com.mall.publisher.entity.KeywordStats;
import com.mall.publisher.mapper.KeywordStatsMapper;
import com.mall.publisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:22
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;
    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
