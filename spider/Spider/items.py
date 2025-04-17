# 数据容器文件

import scrapy

class SpiderItem(scrapy.Item):
    pass

class MingsuxinxiItem(scrapy.Item):
    # 标题
    title = scrapy.Field()
    # 图片
    imgurl = scrapy.Field()
    # 户型
    huxing = scrapy.Field()
    # 出租类型
    chuzutype = scrapy.Field()
    # 宜住
    yizhu = scrapy.Field()
    # 地址
    address = scrapy.Field()
    # 每晚价格
    mwprice = scrapy.Field()
    # 评论数
    commentnum = scrapy.Field()
    # 推荐数
    recommendnum = scrapy.Field()
    # 房间图片数
    picnum = scrapy.Field()
    # 评分
    score = scrapy.Field()
    # 日期
    riqi = scrapy.Field()
    # 详情地址
    detailurl = scrapy.Field()

