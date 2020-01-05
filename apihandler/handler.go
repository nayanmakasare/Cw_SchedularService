package apihandler

import (
	pb "Cw_Schedule/proto"
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
	"time"
)

type Server struct {
	SchedularCollection *mongo.Collection
	RedisConnection     *redis.Client
	TileCollection      *mongo.Collection
}

//type primePage struct {
//	pageName string `json:"pageName"`
//	pageEndpoint string `json:"pageEndpoint"`
//}
//
//func (m *primePage) MarshalBinary() ([]byte, error) {
//	return json.Marshal(m)
//}
//
//type Page struct {
//	carouselEndpoint string  `json:"carouselEndpoint"`
//	rowContentEndpoint []string  `json:"rowContentEndpoint"`
//}
//
//func (m *Page) MarshalBinary() ([]byte, error) {
//	return json.Marshal(m)
//}
//
//type carosuel struct{
//	imageUrl string `json:"imageUrl"`
//	target string `json:"target"`
//	title string `json:"title"`
//	packageName string `json:"packageName"`
//}
//
//func (m *carosuel) MarshalBinary() ([]byte, error) {
//	return json.Marshal(m)
//}
//

type IntermTile struct {
	ID struct {
		CreatedAt struct {
			Date struct {
				NumberLong string `json:"$numberLong"`
			} `json:"$date"`
		} `json:"created_at"`
		ReleaseDate string `json:"releaseDate"`
		Year        string `json:"year"`
		Rating      struct {
			NumberDouble string `json:"$numberDouble"`
		} `json:"rating"`
		Title        string   `json:"title"`
		ContentID    string   `json:"contentId"`
		PackageName  string   `json:"packageName"`
		Portrait     []string `json:"portrait"`
		Poster       []string `json:"poster"`
		IsDetailPage bool     `json:"isDetailPage"`
		Target       []string `json:"target"`
	} `json:"_id"`
}

type Temp struct {
	ID struct {
		CreatedAt struct {
			Date struct {
				NumberLong string `json:"$numberLong"`
			} `json:"$date"`
		} `json:"created_at"`
		ReleaseDate string `json:"releaseDate"`
		Year        string `json:"year"`
	} `json:"_id"`
	ContentTile []struct {
		Title        string   `json:"title"`
		Portrait     []string `json:"portrait"`
		Poster       []string `json:"poster"`
		ContentID    string   `json:"contentId"`
		IsDetailPage bool     `json:"isDetailPage"`
		PackageName  string   `json:"packageName"`
		Target       []string `json:"target"`
	} `json:"contentTile"`
}

//
//
//type row struct {
//	rowName string `json:"rowName"`
//	rowLayout pb.RowLayout `json:"rowLayout"`
//	rowBaseUrl string `json:"rowBaseUrl"`
//	contentTile []contentTile `json:"contentTiles"`
//}
//
//
//func (m *row) MarshalBinary() ([]byte, error) {
//	return json.Marshal(m)
//}

func (s *Server) CreateSchedule(ctx context.Context, req *pb.Schedule) (*pb.Schedule, error) {

	//making fulter query
	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}, {"starttime": req.GetStartTime()}, {"endtime": req.GetEndTime()}}}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(ctx, filter)

	if findResult.Err() != nil {
		log.Println(findResult.Err())
		//All ok now insert the schedule
		ts, _ := ptypes.TimestampProto(time.Now())
		req.CreatedAt = ts
		_, err := s.SchedularCollection.InsertOne(ctx, req)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Mongo error while inserting schedule %s ", err.Error()))
		}
		return req, nil
	}
	return nil, status.Error(codes.AlreadyExists, "schedule already exits please call update api instead")
}

func (s *Server) GetSchedule(req *pb.GetScheduleRequest, stream pb.SchedularService_GetScheduleServer) error {
	//gettting current hour
	hours, _, _ := time.Now().Clock()

	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	//filter := bson.M{"$and" : []bson.M{bson.M{"brand" : req.GetBrand()}, bson.M{"vendor" :req.GetVendor()} , bson.M{"starttime": bson.M{"$lte" : hours}}, bson.M{"endtime" :  bson.M{"$gt" : hours}} }}

	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}}}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(stream.Context(), filter)

	if findResult.Err() != nil {
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}

	//decoding document in to struct
	var schedule pb.Schedule
	err := findResult.Decode(&schedule)
	if err != nil {
		log.Println(err)
		return status.Error(codes.Internal, fmt.Sprintf("Error in decoding Schedule "))
	}
	//sending stream
	return stream.Send(&schedule)
}

func (s *Server) UpdateSchedule(ctx context.Context, req *pb.Schedule) (*pb.Schedule, error) {
	// check if already present

	//gettting current hour
	hours, _, _ := time.Now().Clock()

	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}, {"starttime": bson.M{"$lte": hours}}, {"endtime": bson.M{"$gt": hours}}}}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(ctx, filter)

	if findResult.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}

	//decoding document in to struct
	var schedule pb.Schedule
	err := findResult.Decode(&schedule)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error in decoding Schedule "))
	}
	ts, _ := ptypes.TimestampProto(time.Now())
	schedule.UpdatedAt = ts
	_, err = s.SchedularCollection.ReplaceOne(ctx, filter, schedule)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error while updating Schedule in DB for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}
	return &schedule, nil
}

func (s *Server) DeleteSchedule(ctx context.Context, req *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	hours, _, _ := time.Now().Clock()
	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}, {"starttime": req.GetStartTime()}, {"endtime": req.GetEndTime()}}}
	deleteResult := s.SchedularCollection.FindOneAndDelete(ctx, filter)

	if deleteResult.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}
	return &pb.DeleteScheduleResponse{IsSuccessful: true}, nil
}

func (s *Server) RefreshSchedule(ctx context.Context, req *pb.RefreshScheduleRequest) (*pb.RefreshScheduleResponse, error) {

	log.Println("hit 1")

	filter := bson.M{"$and": []bson.M{{"brand": "shinko"}, {"vendor": "cvte"}}}

	findResult := s.SchedularCollection.FindOne(ctx, filter)
	if findResult.Err() != nil {
		return nil, findResult.Err()
	}
	var schedule pb.Schedule
	err := findResult.Decode(&schedule)
	if err != nil {
		return nil, err
	}

	primeKey := fmt.Sprintf("%s:%s:cloudwalkerPrimePages", formatString(schedule.Vendor), formatString(schedule.Brand))

	log.Println("Prime Key =============>  ", primeKey)

	//pages
	for _, pageValue := range schedule.Pages {

		var pageObj pb.HelperPage

		// TODO Adding Carousel To Redis..... DONE !!!
		if len(pageValue.Carousel) > 0 {

			carouselKey := fmt.Sprintf("%s:%s:%s:carousel", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName))

			log.Println("carosuel Key ==================>  ", carouselKey)

			// getting carousel
			for _, carouselValues := range pageValue.Carousel {

				carobj := pb.HelperCarosuel{
					ImageUrl:    carouselValues.ImageUrl,
					Target:      carouselValues.Target,
					Title:       carouselValues.Title,
					PackageName: carouselValues.PackageName,
				}

				resultByteArray, err := proto.Marshal(&carobj)
				if err != nil {
					return nil, err
				}

				// setting page carousel in redis
				result := s.RedisConnection.SAdd(carouselKey, resultByteArray)

				if result.Err() != nil {
					log.Fatal(result.Err())
				}
			}

			pageObj.CarouselEndpoint = fmt.Sprintf("/carousel/%s/%s/%s", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName))
		}

		//TODO Adding Rows to Redis.....Done !!
		var rowPathSet []string

		// getting rows
		for _, rowValues := range pageValue.GetRow() {

			rowKey := fmt.Sprintf("%s:%s:%s:%s", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName),
				formatString(rowValues.RowName))

			// making stages
			pipeline := pipelineMaker(rowValues)

			// creating aggregation query
			tileCur, err := s.TileCollection.Aggregate(context.Background(), pipeline)
			if err != nil {
				log.Fatal(err)
			}

			var contentTiles []*pb.HelperContentTile
			defer tileCur.Close(ctx)

			var contentTile pb.HelperContentTile
			var temp Temp
			for tileCur.Next(context.Background()) {
				err = tileCur.Decode(&temp)
				if err != nil {
					return nil, err
				}
				contentTile.ContentId = temp.ContentTile[0].ContentID
				contentTile.IsDetailPage = temp.ContentTile[0].IsDetailPage
				contentTile.PackageName = temp.ContentTile[0].PackageName
				contentTile.Poster = temp.ContentTile[0].Poster
				contentTile.Portrait = temp.ContentTile[0].Portrait
				contentTile.Target = temp.ContentTile[0].Target

				contentTiles = append(contentTiles, &contentTile)
			}

			log.Println("RowKey  ===============> ", rowKey)

			helperRow := pb.HelperRow{
				RowName:     rowValues.RowName,
				Rowlayout:   rowValues.Rowlayout,
				RowBaseUrl:  "http://cloudwalker-assets-prod.s3.ap-south-1.amazonaws.com/images/tiles/",
				ContentTile: contentTiles,
			}

			//log.Println("row proto   ============>  ",helperRow.String())

			resultByteArray, err := proto.Marshal(&helperRow)
			if err != nil {
				return nil, err
			}

			//log.Println("row bytes ===============> ", resultByteArray)

			//TODO add base Url to it
			s.RedisConnection.SAdd(rowKey, resultByteArray)

			rowPathSet = append(rowPathSet, fmt.Sprintf("/row/%s/%s/%s/%s", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName),
				formatString(rowValues.RowName)))
		}

		//TODO Pages storing to redis...
		pageObj.RowContentEndpoint = rowPathSet

		resultByteArray, err := proto.Marshal(&pageObj)
		if err != nil {
			return nil, err
		}

		pageKey := fmt.Sprintf("%s:%s:%s", formatString(schedule.Vendor),
			formatString(schedule.Brand),
			formatString(pageValue.PageName))

		s.RedisConnection.SAdd(pageKey, resultByteArray)

		//log.Println("Page info ", pageValue.PageName, " ", fmt.Sprintf("%s/%s/%s", 	formatString(schedule.Vendor),
		//																						formatString(schedule.Brand),
		//																						formatString(pageValue.PageName)))

		primePageObj := pb.PrimePages{
			PageName: pageValue.PageName,
			PageEndpoint: fmt.Sprintf("/page/%s/%s/%s", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName)),
		}

		log.Println("Page Key ===========>   ", pageKey)

		resultByteArray, err = proto.Marshal(&primePageObj)
		if err != nil {
			return nil, err
		}

		//setting prime pages in redis
		result := s.RedisConnection.SAdd(primeKey, resultByteArray)
		if result.Err() != nil {
			log.Println("hit 12")
			return nil, result.Err()
		}
	}

	return &pb.RefreshScheduleResponse{}, nil
}

func formatString(value string) string {
	return strings.ToLower(strings.Replace(value, " ", "_", -1))
}

func pipelineMaker(rowValues *pb.Row) mongo.Pipeline {
	// creating pipes for mongo aggregation
	pipeline := mongo.Pipeline{}

	var filterArray []bson.E
	//pipeline = append(pipeline , bson.D{{"$match", bson.D{{"content.publishState", true}}}})
	if rowValues.RowType == pb.RowType_Editorial {
		// Adding stages 1
		pipeline = append(pipeline, bson.D{{"$match", bson.D{{"ref_id", bson.D{{"$in", rowValues.RowTileIds}}}}}})


		//Adding stage 2
		pipeline = append(pipeline, bson.D{{"$project", bson.D{{"title", "$metadata.title"},
			{"portrait", "$posters.portrait",},
			{"poster", "$posters.landscape"},
			{"contentId", "$ref_id"},
			{"isDetailPage", "$content.detailPage"},
			{"packageName", "$content.package"},
			{"target", "$content.target"},}}})


	} else {
		for key, value := range rowValues.RowFilters {
			filterArray = append(filterArray, bson.E{key, bson.D{{"$in", value.Values}}})
		}
		// Adding stages 1
		pipeline = append(pipeline, bson.D{{"$match", filterArray}})

		// making stage 2
		stage2 := bson.D{{"$group", bson.D{{"_id", bson.D{
			{"created_at", "$created_at"},
			{"releaseDate", "$metadata.releaseDate"},
			{"year", "$metadata.year"},
		}}, {"contentTile", bson.D{{"$push", bson.D{
				{"title", "$metadata.title"},
				{"portrait", "$posters.portrait",},
				{"poster", "$posters.landscape"},
				{"contentId", "$ref_id"},
				{"isDetailPage", "$content.detailPage"},
				{"packageName", "$content.package"},
				{"target", "$content.target"},
		}}}}}}}

		pipeline = append(pipeline, stage2)

		// making stage 3
		var sortArray []bson.E
		for key, value := range rowValues.RowSort {
			sortArray = append(sortArray, bson.E{fmt.Sprintf("_id.%s", key), value})
		}

		//stage 3
		stage3 := bson.D{{"$sort", sortArray}}
		pipeline = append(pipeline, stage3)
	}

	return pipeline
}
