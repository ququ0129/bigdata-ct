package com.atguigu.ct.web.service.impl;

import com.atguigu.ct.web.bean.Calllog;
import com.atguigu.ct.web.dao.CalllogDao;
import com.atguigu.ct.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CalllogServiceImpl implements CalllogService {
    @Autowired
    private CalllogDao calllogDao;

    @Override
    public List<Calllog> queryMonthDatas(String tel, String calltime) {
        Map<String,Object> paraMap = new HashMap<String,Object>();
        paraMap.put("tel",tel);

        if(calltime.length()>4){
            calltime = calltime.substring(0,4);
        }
        paraMap.put("year",calltime);
        return calllogDao.queryMonthDatas(paraMap);
    }
}
