package com.mall.publisher.mapper;

import com.mall.publisher.entity.ProvinceStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author KevenHe
 * @create 2022/1/23 13:08
 */
@Mapper
public interface ProvinceStatsMapper {
    //按地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount " +
            "from province_stats_2021 where toYYYYMMDD(stt)=#{date} " +
            "group by province_id ,province_name ")
    public List<ProvinceStats> selectProvinceStats(int date);
}