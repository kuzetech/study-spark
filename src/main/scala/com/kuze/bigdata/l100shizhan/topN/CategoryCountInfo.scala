package com.kuze.bigdata.l100shizhan.topN

/**
 * CategoryCountInfo
 *
 * @author baiye
 * @date 2020/9/1 下午3:56
 */
case class CategoryCountInfo(categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)
