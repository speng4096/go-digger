package gaodemap

import (
	"fmt"
	"github.com/antonholmquist/jason"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spencer404/go-digger"
	"github.com/spencer404/go-digger/storage"
	"github.com/spencer404/go-digger/tool"
	"log"
	"strconv"
	"strings"
)

const pageSize = 20

type Point struct {
	Lng float64
	Lat float64
}
type GaodePOI struct {
	Point
	ID           string
	Name         string
	Type         string
	TypeCode     string
	Tel          string
	Province     string
	ProvinceCode string
	City         string
	CityCode     string
	Area         string
	AreaCode     string
	Address      string
}

var headers = map[string]string{
	"Referer": "https://lbs.amap.com/api/javascript-api/example/poi-search/polygon-search",
	"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) " +
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36",
}

func makeURL(point1, point2 Point, keyword, areaCode string) string {
	s := fmt.Sprintf("%f,%f|%f,%f|%s|%s", point1.Lng, point1.Lat, point2.Lng, point2.Lat, keyword, areaCode)
	return s
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

func parsePOI(content *jason.Object) (poi GaodePOI, err error) {
	if poi.ID, err = content.GetString("id"); err != nil {
		return GaodePOI{}, errors.Wrap(err, "解析'id'失败")
	}
	if poi.Name, err = content.GetString("name"); err != nil {
		return GaodePOI{}, errors.Wrap(err, "解析'name'失败")
	}
	if poi.Type, err = content.GetString("type"); err != nil {
		poi.Type = ""
	}
	if poi.TypeCode, err = content.GetString("typecode"); err != nil {
		poi.TypeCode = ""
	}
	if poi.Tel, err = content.GetString("tel"); err != nil {
		poi.Tel = ""
	}
	if poi.Province, err = content.GetString("pname"); err != nil {
		poi.Province = ""
	}
	if poi.ProvinceCode, err = content.GetString("pcode"); err != nil {
		poi.ProvinceCode = ""
	}
	if poi.City, err = content.GetString("cityname"); err != nil {
		poi.City = ""
	}
	if poi.CityCode, err = content.GetString("citycode"); err != nil {
		poi.CityCode = ""
	}
	if poi.Area, err = content.GetString("adname"); err != nil {
		poi.Area = ""
	}
	if poi.AreaCode, err = content.GetString("adcode"); err != nil {
		poi.AreaCode = ""
	}
	if poi.Address, err = content.GetString("address"); err != nil {
		poi.Address = ""
	}
	// Point
	location, err := content.GetString("location")
	if err != nil {
		return GaodePOI{}, errors.Wrap(err, "解析'location'失败")
	}
	item := strings.Split(location, ",")
	if len(item) == 2 {
		if poi.Point.Lng, err = strconv.ParseFloat(item[0], 10); err != nil {
			return GaodePOI{}, errors.Wrap(err, "解析'lng失败")
		}
		if poi.Point.Lat, err = strconv.ParseFloat(item[1], 10); err != nil {
			return GaodePOI{}, errors.Wrap(err, "解析'lat'失败")
		}
	}
	return poi, nil
}

func NewGaodePOISearchSpider(keyword string, onSave func(poi GaodePOI)) *digger.Spider {
	return &digger.Spider{
		// 坐标为大陆矩形范围
		Seeders: []string{
			makeURL(Point{72.396497, 0.957873}, Point{138.332409, 54.684761}, keyword, "全国"),
		},
		OnProcess: func(url string, client *resty.Client, reactor *digger.Reactor) error {
			// 获取参数
			point1, point2, keyword, areaCode, err := parseURL(url)
			if err != nil {
				return err
			}
			// 确保point2在point1右上方
			if point1.Lng >= point2.Lng || point1.Lat >= point2.Lat {
				log.Println("忽略错误的矩形", point1, point2)
				return nil
			}
			// 请求接口
			polygon := fmt.Sprintf("%f,%f;%f,%f;%f,%f;%f,%f",
				point1.Lng, point2.Lat, point2.Lng, point2.Lat, point2.Lng, point1.Lat, point1.Lng, point1.Lat,
			)
			params := map[string]string{
				"polygon":    polygon,
				"s":          "rsv3",
				"children":   "",
				"key":        "608d75903d29ad471362f8c58c550daf",
				"city":       "",
				"citylimit":  "false",
				"extensions": "all",
				"language":   "undefined",
				"callback":   "jsonp_33457_",
				"platform":   "JS",
				"logversion": "2.0",
				"appname":    "https://lbs.amap.com/api/javascript-api/example/poi-search/polygon-search",
				"sdkversion": "1.4.15",
				"keywords":   keyword,
				"page":       "1",
				"pageSize":   fmt.Sprintf("%d", pageSize),
			}
			req := client.R().SetQueryParams(params).SetHeaders(headers)
			resp, err := req.Get("https://restapi.amap.com/v3/place/polygon")
			if err != nil {
				return err
			}
			if resp.StatusCode() != 200 {
				return errors.Errorf("接口返回异常状态码: %d", resp.StatusCode())
			}
			// 提取JSON数据
			obj, err := tool.ParseJSONp(resp.String())
			if err != nil {
				if strings.Contains(resp.String(), "errcode:30000") {
					return errors.Errorf("IP已被限制")
				} else {
					return err
				}
			}
			contents, err := obj.GetObjectArray("pois")
			if err != nil {
				log.Println("无搜索结果")
				return nil // 无搜索结果
			}
			// 若结果数量过多，则需细化搜索范围
			if len(contents) >= pageSize {
				log.Printf("结果数量:%d, 分割搜索区域", len(contents))
				pointA1, pointA2, pointB1, pointB2 := splitArea(point1, point2)
				_, err = reactor.Queue.Add(makeURL(pointA1, pointA2, keyword, areaCode), storage.Priority3)
				if err != nil {
					return err
				}
				_, err = reactor.Queue.Add(makeURL(pointB1, pointB2, keyword, areaCode), storage.Priority3)
				if err != nil {
					return err
				}
			}
			// 结构化数据
			log.Printf("结果数量:%d, 正在保存", len(contents))
			for _, content := range contents {
				poi, err := parsePOI(content)
				if err != nil {
					log.Printf("解析POI失败: %s\n--Content:%s", err, content)
					continue
				}
				onSave(poi)
			}
			return nil
		},
	}
}
