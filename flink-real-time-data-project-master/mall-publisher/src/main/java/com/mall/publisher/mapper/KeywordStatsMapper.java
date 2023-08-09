package com.mall.publisher.mapper;

import com.mall.publisher.entity.KeywordStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:21
 */
@Mapper
public interface KeywordStatsMapper {
    @Select("select keyword," +
            "sum(keyword_stats_2021.ct * " +
            "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct"
            +
            " from keyword_stats_2021 where toYYYYMMDD(stt)=#{date} group by keyword " +
            "order by sum(keyword_stats_2021.ct) desc limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int
            limit);
}