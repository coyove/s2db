package fts

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

var text = `河北固安发现2名阳性人员，系共同居住的母女
    31省区市新增确诊病例27例 其中本土病例9例
    北京2月4日新增1例本土确诊病例 在丰台区
    深圳2地升级中风险 全国共有高中风险区7+56个
    杭州疫情出现拐点，已从社区清零转向隔离点清零
    路透社记者给北京防疫找“茬”，推特网友群嘲
    深圳暂停全市线下校外培训服务和托管服务
    广西德保县公布1例阳性病例活动轨迹
	

    习主席的一天（2022年2月5日）
    民生有保障 看习近平的“天下一家”情怀
    “载入史册的盛会” 恢宏画卷 为突破喝彩 冬奥年
    这行泪，为国家荣耀而流淌 6日观赛指南：苏翊鸣亮相
    “鸟巢”和世界都安静了 中国为什么能赢得金牌
    新春走基层｜三江之源，他们守望家园
    北京冬奥会开幕式 令人眼见活力心生希望
    交通部门以科技护航冬奥春运出行安全

    交通部门以科技护航冬奥春运出行安全
    “感动中国”教师刘芳：母爱，是心中的一道光
    俄奥委会主席：北京冬奥会开幕式简洁而又精彩
    31省区市新增确诊病例43例 其中本土病例13例
    就在今晚！中国女足冲击亚洲杯冠军
    1月“调控”超66次，中国各地稳楼市政策频出
    队魂就是“拼命滑”！你永远可以相信中国短道队
    美国就这么看中俄联合声明？
    人少的代表团为什么都参赛高山滑雪？
    北京累计报告118例本土病例，分布丰台朝阳等多区
    “拉面馆”开到疫情防控卡口 暖胃又暖心

    财经 | 央企董事长、全国优秀企业家向全国人民拜年
    冬奥开幕式霸屏！微火火炬等酷科技，背后是这些公司
    实探春节楼市:中介放假 贷款额度充裕 客户主动求带看
    股票| 百亿基金经理如何看这十大投资问题？ 开户福利

    起底零跑汽车：IT人造车 2021房企品牌价值50强揭晓

    科技 | 小学生“最惨假期”？游戏防沉迷落地效果如何
    张艺谋全方位揭秘北京冬奥会开幕式：呈现一种现代感
    相亲对象太奇葩，可能得怪你爸妈 GIF:姿势最奇怪的狗

    汽车 | 特斯拉拟扩建奥斯汀工厂 生产电池阴极

    本地 | 上海铁路春运进入节前最高峰 双向过节为团圆
    上海众多新地标载体迎首个春节 花式秀年味掀消费热潮
    关注 | 隈研吾设计酒店7选 新手表很快就戴腻了

    必看 | 真香开箱：牛年12生肖幸运色 2021整体运势
`

func TestTokenizerAddRemove(t *testing.T) {
	MaxTokenDocIDs = 100

	rand.Seed(time.Now().Unix())
	db, _ := Open("test")
	idx := db.Open("name")
	defer db.Close()

	start := time.Now()
	m := map[uint32]string{}
	const N = 1e3
	for i := 0; i < N; i++ {
		start := rand.Intn(len(text) / 2)
		end := start + rand.Intn(len(text)/2-1)
		m[uint32(i)] = text[start : end+1]
		if i%100 == 99 {
			cnt := idx.Index(m)
			m = map[uint32]string{}
			fmt.Println("index", i, cnt)
		}
	}
	// fmt.Println(idx.TopN(true, 10, "中国"))
	m = map[uint32]string{}
	for ii, i := range rand.Perm(N) {
		m[uint32(i)] = ""
		if ii%100 == 99 {
			idx.Index(m)
			m = map[uint32]string{}
			fmt.Println("remove", ii)
		}
	}
	if s := idx.Stat(); s.NumDocuments != 0 || s.NumTokens != 0 {
		t.Fatal(s.NumDocuments, s.NumTokens)
	}
	fmt.Println(time.Since(start))
}

func TestTokenizer(t *testing.T) {
	db, _ := Open("test")
	idx := db.Open("name")

	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fmt.Println(i, idx.Index(map[uint32]string{uint32(i): line}))
	}

	fmt.Println(idx.TopN(true, 100, "中国"))
}
