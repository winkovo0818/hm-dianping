package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.impl.FollowServiceImpl;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {
    @Resource
    private FollowServiceImpl followService;
    /**
     * 关注功能
     */
    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable Long id,@PathVariable Boolean isFollow){
        return followService.follow(id,isFollow);
    }

    /**
     * 判断是否关注
     */
    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable Long id){
        return followService.isFollow(id);
    }
    /**
     * 共同关注
     */
    @GetMapping("/common/{id}")
    public Result commonFollow(@PathVariable Long id){
        return followService.commonFollow(id);
    }
}
