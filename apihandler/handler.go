package apihandler

import (
	pb "Cw_Schedule/proto"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
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
	RedisConnection  *redis.Client
	TileCollection *mongo.Collection
}

type primePage struct {
	pageName string `json:"pageName"`
	pageEndpoint string `json:"pageEndpoint"`
}

func (m *primePage) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

type Page struct {
	carouselEndpoint string  `json:"carouselEndpoint"`
	rowContentEndpoint []string  `json:"rowContentEndpoint"`
}

func (m *Page) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

type carosuel struct{
	imageUrl string `json:"imageUrl"`
	target string `json:"target"`
	title string `json:"title"`
	packageName string `json:"packageName"`
}

func (m *carosuel) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

type contentTile struct {
	title string `json:"_id.title"`
	portrait []string `json:"_id.portrait"`
	poster []string `json:"_id.poster"`
	contentId string `json:"_id.contentId"`
	isDetailPage bool `json:"_id.isDetailPage"`
	target []string `json:"_id.target"`
	packageName string `json:"_id.packageName"`
}

func (m *contentTile) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}


type row struct {
	rowName string `json:"rowName"`
	rowLayout pb.RowLayout `json:"rowLayout"`
	rowBaseUrl string `json:"rowBaseUrl"`
	contentTile []contentTile `json:"contentTiles"`
}


func (m *row) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}


func(s *Server) CreateSchedule(ctx context.Context, req *pb.Schedule) (*pb.Schedule, error){

	//making fulter query
	filter := bson.M{"$and" : []bson.M{bson.M{"brand" : req.GetBrand()}, bson.M{"vendor" :req.GetVendor()} , bson.M{"starttime": req.GetStartTime()}, bson.M{"endtime" : req.GetEndTime()} }}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(ctx, filter)

	if findResult.Err() != nil {
		log.Println(findResult.Err())
		//All ok now insert the schedule
		ts, _ := ptypes.TimestampProto(time.Now())
		req.CreatedAt = ts
		_, err := s.SchedularCollection.InsertOne(ctx, req)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Mongo error while inserting schedule %s ",err.Error()))
		}
		return req, nil
	}
	return nil, status.Error(codes.AlreadyExists, "schedule already exits please call update api instead")
}

func(s *Server) GetSchedule(req *pb.GetScheduleRequest, stream pb.SchedularService_GetScheduleServer) error {
	//gettting current hour
	hours, _, _ := time.Now().Clock()

	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	//filter := bson.M{"$and" : []bson.M{bson.M{"brand" : req.GetBrand()}, bson.M{"vendor" :req.GetVendor()} , bson.M{"starttime": bson.M{"$lte" : hours}}, bson.M{"endtime" :  bson.M{"$gt" : hours}} }}



	filter := bson.M{"$and" : []bson.M{bson.M{"brand" : req.GetBrand()}, bson.M{"vendor" :req.GetVendor()} }}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(stream.Context(), filter)

	if findResult.Err() != nil {
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ",req.Brand, req.Vendor, hours))
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

func(s *Server) UpdateSchedule(ctx context.Context, req *pb.Schedule)  (*pb.Schedule, error){
	// check if already present

	//gettting current hour
	hours, _, _ := time.Now().Clock()

	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	filter := bson.M{"$and" : []bson.M{bson.M{"brand" : req.GetBrand()}, bson.M{"vendor" :req.GetVendor()} , bson.M{"starttime": bson.M{"$lte" : hours}}, bson.M{"endtime" :  bson.M{"$gt" : hours}} }}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(ctx, filter)

	if findResult.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ",req.Brand, req.Vendor, hours))
	}

	//decoding document in to struct
	var schedule pb.Schedule
	err := findResult.Decode(&schedule)
	if err != nil {
		log.Println(err)
		return nil,status.Error(codes.Internal, fmt.Sprintf("Error in decoding Schedule "))
	}
	ts, _ := ptypes.TimestampProto(time.Now())
	schedule.UpdatedAt = ts
	_, err = s.SchedularCollection.ReplaceOne(ctx, filter, schedule)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error while updating Schedule in DB for brand  %s and vendor %s at time hour %d ",req.Brand, req.Vendor, hours))
	}
	return &schedule, nil
}

func (s *Server) DeleteSchedule(ctx context.Context, req *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	hours, _, _ := time.Now().Clock()
	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	filter := bson.M{"$and": []bson.M{bson.M{"brand": req.GetBrand()}, bson.M{"vendor": req.GetVendor()}, bson.M{"starttime": req.GetStartTime()}, bson.M{"endtime": req.GetEndTime()}}}
	deleteResult := s.SchedularCollection.FindOneAndDelete(ctx, filter)

	if deleteResult.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}
	return &pb.DeleteScheduleResponse{IsSuccessful: true}, nil
}


func (s *Server) RefreshSchedule(ctx context.Context, req *pb.RefreshScheduleRequest) (*pb.RefreshScheduleResponse, error) {

	log.Println("hit 1")

	filter := bson.M{"$and" : []bson.M{bson.M{"brand" : "shinko"}, bson.M{"vendor" :"cvte"}}}
	findResult := s.SchedularCollection.FindOne(ctx, filter)
	if findResult.Err() != nil {
		return nil, findResult.Err()
	}
	var schedule pb.Schedule
	err  := findResult.Decode(&schedule)
	if err != nil {
		return nil, err
	}

	//pages
	for _ , pageValue := range schedule.Pages {

		carouselKey := fmt.Sprintf("%s:%s:%s:carousel",	formatString(schedule.Vendor),
																formatString(schedule.Brand),
																formatString(pageValue.PageName), )

		var page Page

		if len(pageValue.Carousel) > 0 {

			// getting carousel
			for _, carouselValues := range pageValue.Carousel {
				// setting page carousel in redis
				result := s.RedisConnection.SAdd(carouselKey ,  &carosuel{
					imageUrl:    carouselValues.ImageUrl,
					target:      carouselValues.Target,
					title:       carouselValues.Title,
					packageName: carouselValues.PackageName,
				})

				if result.Err() != nil {
					log.Fatal(result.Err())
				}
			}

			page.carouselEndpoint = fmt.Sprintf("%s/%s/%s/carousel", 	formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName),)
		}


		var rowPathSet []string

		// getting rows
		for _, rowValues := range pageValue.GetRow() {
			rowKey := fmt.Sprintf("%s:%s:%s:%s",		formatString(schedule.Vendor),
															formatString(schedule.Brand),
															formatString(pageValue.PageName),
															formatString(rowValues.RowName),
								)

			// making stages
			pipeline := pipelineMaker(rowValues)

			// creating aggregation query
			tileCur, err := s.TileCollection.Aggregate(context.Background(), pipeline)
			if(err != nil){
				log.Fatal(err)
			}

			var contentTiles []contentTile
			defer tileCur.Close(ctx)
			for tileCur.Next(context.Background()){
				var contentTile contentTile
				err = tileCur.Decode(&contentTile)
				if err != nil {
					log.Fatal(err)
				}
				contentTiles = append(contentTiles, contentTile)
			}

			//TODO add base Url to it
			s.RedisConnection.SAdd(rowKey, &row{
				rowName:    rowValues.RowName,
				rowLayout:  rowValues.Rowlayout,
				contentTile: contentTiles,
			})

			rowPathSet = append(rowPathSet,  fmt.Sprintf("%s/%s/%s/%s",		formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName),
				formatString(rowValues.RowName),
			))
		}

		page.rowContentEndpoint = rowPathSet

		pageKey := fmt.Sprintf("%s:%s:%s", formatString(schedule.Vendor),
													formatString(schedule.Brand),
													formatString(pageValue.PageName))

		s.RedisConnection.SAdd(pageKey, page)

		log.Println("Page info ", pageValue.PageName, " ", fmt.Sprintf("%s/%s/%s", 	formatString(schedule.Vendor),
			formatString(schedule.Brand),
			formatString(pageValue.PageName)))


		primePage := primePage{
			pageName:     pageValue.PageName,
			pageEndpoint: fmt.Sprintf("%s/%s/%s", 	formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName)),
		}

		resultByte , err := json.Marshal(primePage)
		if err != nil {
			return nil, err
		}

		//setting prime pages in redis
		result := s.RedisConnection.SAdd(fmt.Sprintf("%s:%s:cloudwalkerPrimePages", formatString(schedule.Vendor), formatString(schedule.Brand)), string(resultByte) )
		if result.Err() != nil {
			log.Println("hit 12")
			return nil, result.Err()
		}
	}



	return &pb.RefreshScheduleResponse{}, nil
}

func formatString(value string) string  {
	return  strings.ToLower(strings.Replace(value, " ", "_", -1))
}



func pipelineMaker(rowValues *pb.Row) mongo.Pipeline {
	// creating pipes for mongo aggregation
	pipeline := mongo.Pipeline{}

	var filterArray []bson.E
	if rowValues.RowType == pb.RowType_Editorial {
		// Adding stages 1
		pipeline = append(pipeline , bson.D{{"$match", bson.D{{"ref_id", bson.D{{"$in", rowValues.RowTileIds}}}}}})
	}else{
		for key, value := range rowValues.RowFilters {
			filterArray = append(filterArray, bson.E{key, bson.D{{"$in", value.Values}}} )
		}
		// Adding stages 1
		pipeline = append(pipeline, bson.D{{"$match", filterArray}})
	}

	// making stage 2
	stage2 := bson.D{{"$group", bson.D{{"_id", bson.D{
		{"created_at", "$created_at"},
		{"releaseDate", "$metadata.releaseDate"},
		{"year", "$metadata.year"},
		{"rating", "$metadata.rating"},
		{"title", "$metadata.title"},
		{"contentId", "$ref_id"},
		{"packageName", "$content.package"},
		{"portrait", "$posters.portrait"},
		{"poster", "$posters.landscape"},
		{"isDetailPage", "$content.detailPage"},
		{"target", "$content.target"},
	}}}}}

	pipeline = append(pipeline, stage2)


	// making stage 3
	var sortArray []bson.E
	for key, value := range rowValues.RowSort {
		sortArray = append(sortArray, bson.E{fmt.Sprintf("_id.%s", key), value})
	}


	//stage 3
	stage3 := bson.D{{"$sort", sortArray}}
	pipeline = append(pipeline, stage3)

	return  pipeline
}


































