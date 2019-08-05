package spiders

import (
	"fmt"
	"github.com/antonholmquist/jason"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spencer404/go-bd09mc"
	"github.com/spencer404/go-digger"
	"github.com/spencer404/go-digger/storage"
	"github.com/spencer404/go-digger/tools"
	"log"
	"strconv"
	"strings"
)

type Point struct {
	Lng float64
	Lat float64
}
type BaiduPOI struct {
	Point
	UID         string
	Name        string
	Province    string
	City        string
	CityID      int64
	Area        string
	AreaID      int64
	Tag         string
	TagID       string
	Address     string
	AddressNorm string
	Tel         string
}

var headers = map[string]string{
	"Referer": "http://lbsyun.baidu.com/jsdemo.htm",
	"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) " +
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",
}

func makeURL(point1, point2 Point, keyword, areaCode string) string {
	s := fmt.Sprintf("%f,%f|%f,%f|%s|%s", point1.Lng, point1.Lat, point2.Lng, point2.Lat, keyword, areaCode)
	return s
}

func parsePoint(s string) (Point, error) {
	items := strings.Split(s, ",")
	if len(items) != 2 {
		return Point{}, errors.Errorf("Point格式有误: %s", s)
	}
	lng, err := strconv.ParseFloat(items[0], 10)
	if err != nil {
		return Point{}, errors.Errorf("Point格式有误: %s", s)
	}
	lat, err := strconv.ParseFloat(items[1], 10)
	if err != nil {
		return Point{}, errors.Errorf("Point格式有误: %s", s)
	}
	return Point{lng, lat}, nil
}

func parseURL(url string) (point1, point2 Point, keyword, areaCode string, err error) {
	items := strings.Split(url, "|")
	if len(items) != 4 {
		err = errors.Errorf("URL格式有误: %s", url)
		return
	}
	point1, err = parsePoint(items[0])
	if err != nil {
		err = errors.Wrapf(err, "URL格式有误: %s", url)
		return
	}
	point2, err = parsePoint(items[1])
	if err != nil {
		err = errors.Wrapf(err, "URL格式有误: %s", url)
		return
	}
	keyword = items[2]
	areaCode = items[3]
	return
}

func parseJSONp(body string) (*jason.Object, error) {
	s := tools.Centre(body, "{", "}")
	obj, err := jason.NewObjectFromReader(strings.NewReader(s))
	if err != nil {
		return nil, errors.Wrapf(err, "提取JSON失败")
	}
	return obj, nil
}

// 将矩形按长边切割为两个小矩形
func splitArea(point1, point2 Point) (pointA1, pointA2, pointB1, pointB2 Point) {
	var point3, point4 Point
	deltaLng := point2.Lng - point1.Lng
	deltaLat := point2.Lat - point1.Lat
	if deltaLng > deltaLat {
		d := deltaLng / 2
		point3 = Point{point1.Lng + d, point2.Lat}
		point4 = Point{point2.Lng - d, point1.Lat}
	} else {
		d := deltaLat / 2
		point3 = Point{point2.Lng, point1.Lat + d}
		point4 = Point{point1.Lng, point2.Lat - d}
	}
	return point1, point3, point4, point2
}

func parsePOI(content *jason.Object) (poi BaiduPOI, err error) {
	if poi.UID, err = content.GetString("uid"); err != nil {
		return
	}
	if poi.Name, err = content.GetString("name"); err != nil {
		return
	}
	if poi.City, err = content.GetString("admin_info", "city_name"); err != nil {
		return
	}
	if poi.CityID, err = content.GetInt64("admin_info", "city_id"); err != nil {
		return
	}
	if poi.Area, err = content.GetString("admin_info", "area_name"); err != nil {
		return
	}
	if poi.AreaID, err = content.GetInt64("admin_info", "area_id"); err != nil {
		return
	}
	if poi.Tag, err = content.GetString("std_tag"); err != nil {
		return
	}
	if poi.TagID, err = content.GetString("std_tag_id"); err != nil {
		return
	}
	if poi.Address, err = content.GetString("addr"); err != nil {
		return
	}
	if poi.AddressNorm, err = content.GetString("address_norm"); err != nil {
		return
	}
	if poi.Tel, err = content.GetString("tel"); err != nil {
		poi.Tel = ""
	}
	// Province
	left := strings.Index(poi.AddressNorm, "[")
	right := strings.Index(poi.AddressNorm, "(")
	if right > left {
		poi.Province = poi.AddressNorm[left:right]
	}
	// Point
	x, err := content.GetFloat64("ext", "detail_info", "point", "x")
	if err != nil {
		return
	}
	y, err := content.GetFloat64("ext", "detail_info", "point", "y")
	if err != nil {
		return
	}
	poi.Point.Lng, poi.Point.Lat, err = bd09mc.MC2LL(x, y)
	if err != nil {
		return
	}
	return poi, nil
}

func NewBaiduPOISearchSpider(areaCode string, keyword string, onSave func(poi BaiduPOI)) *digger.Spider {
	return &digger.Spider{
		// 坐标为大陆矩形范围
		Seeders: []string{
			makeURL(Point{72.396497, 0.957873}, Point{138.332409, 54.684761}, keyword, areaCode),
		},
		OnProcess: func(url string, reactor *digger.Reactor) error {
			// 获取参数
			point1, point2, keyword, areaCode, err := parseURL(url)
			if err != nil {
				return err
			}
			// 确保point2在point1右下方
			if point1.Lng >= point2.Lng || point1.Lat >= point2.Lat {
				log.Println("忽略错误的矩形", point1, point2)
				return nil
			}
			x1, y1, err := bd09mc.LL2MC(point1.Lng, point1.Lat)
			if err != nil {
				return err
			}
			x2, y2, err := bd09mc.LL2MC(point2.Lng, point2.Lat)
			if err != nil {
				return err
			}
			// 请求接口
			params := map[string]string{
				"qt":          "bd",
				"c":           areaCode,
				"wd":          keyword,
				"ar":          fmt.Sprintf("(%f,%f;%f,%f)", x1, y1, x2, y2),
				"rn":          "50",
				"l":           "18",
				"ie":          "utf-8",
				"oue":         "1",
				"fromproduct": "jsapi",
				"res":         "api",
				"callback":    "BMap._rd._cbk64211",
				"ak":          "E4805d16520de693a3fe707cdc962045", // 百度Demo找的
			}
			req := resty.New().R().SetQueryParams(params).SetHeaders(headers)
			resp, err := req.Get("http://api.map.baidu.com/")
			if err != nil {
				return err
			}
			if resp.StatusCode() != 200 {
				return errors.Errorf("接口返回异常状态码: %d", resp.StatusCode())
			}
			// 提取JSON数据
			obj, err := parseJSONp(resp.String())
			if err != nil {
				return err
			}
			contents, err := obj.GetObjectArray("content")
			if err != nil {
				log.Println("无搜索结果")
				return nil // 无搜索结果
			}
			// 若结果数量过多，则需细化搜索范围
			if len(contents) == 50 {
				log.Printf("结果数量:%d, 缩小搜索范围", len(contents))
				pointA1, pointA2, pointB1, pointB2 := splitArea(point1, point2)
				_, err = reactor.Queue.Add(makeURL(pointA1, pointA2, keyword, areaCode), storage.Priority3)
				if err != nil {
					return err
				}
				_, err = reactor.Queue.Add(makeURL(pointB1, pointB2, keyword, areaCode), storage.Priority3)
				if err != nil {
					return err
				}
				return nil
			}
			// 结构化数据
			log.Printf("结果数量:%d, 正在保存", len(contents))
			for _, content := range contents {
				poi, err := parsePOI(content)
				if err != nil {
					log.Printf("解析POI失败: %s", err)
					continue
				}
				onSave(poi)
			}
			return nil
		},
	}
}
