package com.hmdp.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("upload")
public class UploadController {

    @PostMapping("blog")
    public Result uploadImage(@RequestParam("file") MultipartFile image) {
      //获取旧，方法新建新，构造并保存新路径，返回，打印
        String originalFilename = image.getOriginalFilename();
        String newFileName = createNewFileName(originalFilename);
        try {
            image.transferTo(new File(SystemConstants.IMAGE_UPLOAD_DIR + newFileName));
            return Result.ok(newFileName);
        } catch (IOException e) {
            throw new RuntimeException("文件上传失败", e);//？
        }
    }
    @GetMapping("/blog/delete")
    public Result deleteBlogImg(@RequestParam("name") String filename) {
        File file = new File(SystemConstants.IMAGE_UPLOAD_DIR, filename);
        if (file.isDirectory()) {
            return Result.fail("错误的文件名称");
        }
        FileUtil.del(file);
        return Result.ok();
    }

    private String createNewFileName(String originalFilename) {
        // 1. 提取文件后缀（如 ".jpg"）
        String suffix = StrUtil.subAfter(originalFilename, ".", true);

        // 2. 生成 UUID 作为主文件名（全局唯一）
        String name = UUID.randomUUID().toString();
        int hash = name.hashCode();
        int d1 = hash & 0xF; // 取哈希值的低 4 位（0~15）
        int d2 = (hash >> 4) & 0xF; // 取哈希值的 4~7 位（0~15）

        // 3. 按哈希值分目录存储（避免单目录文件过多）
        File dir = new File(SystemConstants.IMAGE_UPLOAD_DIR, StrUtil.format("/blogs/{}/{}", d1, d2));
        if (!dir.exists()) {
            dir.mkdirs(); // 自动创建多级目录
        }

        // 最终文件名格式：/blogs/[d1]/[d2]/[UUID].[后缀]
        return StrUtil.format("/blogs/{}/{}/{}.{}", d1, d2, name, suffix);
    }
}
