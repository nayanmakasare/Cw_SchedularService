package apihandler

import (
	pb "Cw_Schedule/proto"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

type Server struct {
	SchedularCollection *mongo.Collection
	RedisConnection     *redis.Client
	TileCollection      *mongo.Collection
}

func (s *Server) CreateSchedule(ctx context.Context, req *pb.Schedule) (*pb.Schedule, error) {

	//making fulter query
	//filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}, {"starttime": req.GetStartTime()}, {"endtime": req.GetEndTime()}}}

	log.Println("Create Schedule hit.")
	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}}}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(ctx, filter)

	log.Println("hit find query")
	if findResult.Err() != nil {
		log.Println(findResult.Err())
		//All ok now insert the schedule
		ts, _ := ptypes.TimestampProto(time.Now())
		req.CreatedAt = ts
		log.Println(req)
		_, err := s.SchedularCollection.InsertOne(ctx, &req)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Mongo error while inserting schedule %s ", err.Error()))
		}
		//go s.RefreshingWorker(req, ctx)
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
		return status.Error(codes.Internal, fmt.Sprintf("Error in decoding Schedule "))
	}
	//sending stream
	return stream.Send(&schedule)
}

func (s *Server) UpdateSchedule(ctx context.Context, req *pb.Schedule) (*pb.Schedule, error) {
	// check if already present

	//gettting current hour
	hours, _, _ := time.Now().Clock()


	//, {"starttime": bson.M{"$lte": hours}}, {"endtime": bson.M{"$gt": hours}}

	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}}}

	//check if document already present
	findResult := s.SchedularCollection.FindOne(ctx, filter)

	if findResult.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}

	//decoding document in to struct
	var schedule pb.Schedule
	err := findResult.Decode(&schedule)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error in decoding Schedule "))
	}
	ts, _ := ptypes.TimestampProto(time.Now())
	schedule.UpdatedAt = ts
	_, err = s.SchedularCollection.ReplaceOne(ctx, filter, schedule)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error while updating Schedule in DB for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}

	go s.RefreshingWorker(&schedule, ctx)

	return &schedule, nil
}

func (s *Server) DeleteSchedule(ctx context.Context, req *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	hours, _, _ := time.Now().Clock()
	//making fulter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
	//filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}, {"starttime": req.GetStartTime()}, {"endtime": req.GetEndTime()}}}

	// only temp
	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}}}


	deleteResult := s.SchedularCollection.FindOneAndDelete(ctx, filter)

	if deleteResult.Err() != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("No Schedule found for brand  %s and vendor %s at time hour %d ", req.Brand, req.Vendor, hours))
	}
	return &pb.DeleteScheduleResponse{IsSuccessful: true}, nil
}

func (s *Server) RefreshSchedule(ctx context.Context, req *pb.RefreshScheduleRequest) (*pb.RefreshScheduleResponse, error) {

	filter := bson.M{"$and": []bson.M{{"brand": req.GetBrand()}, {"vendor": req.GetVendor()}}}

	findResult := s.SchedularCollection.FindOne(ctx, filter)
	if findResult.Err() != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Schedule Not found ", findResult.Err()))
	}
	var schedule pb.Schedule
	err := findResult.Decode(&schedule)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Not able to decode the schedule ", err))
	}
	if err = s.RefreshingWorker(&schedule, ctx); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error in refreshing process ", err))
	}
	return &pb.RefreshScheduleResponse{IsSuccessful:true}, nil
}

func(s Server) RefreshingWorker(schedule *pb.Schedule, ctx context.Context) error {

	primeKey := fmt.Sprintf("%s:%s:cloudwalkerPrimePages", formatString(schedule.Vendor), formatString(schedule.Brand))
	ifExitDelete(primeKey, s.RedisConnection)

	//Looping pages
	for _, pageValue := range schedule.Pages {
		var pageObj pb.Page

		pageKey := fmt.Sprintf("%s:%s:%s", 	formatString(schedule.Vendor),
													formatString(schedule.Brand),
													formatString(pageValue.PageName))

		ifExitDelete(pageKey, s.RedisConnection)


		// looping carosuel
		if len(pageValue.Carousel) > 0 {

			carouselKey := fmt.Sprintf("%s:%s:%s:carousel", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName))
			ifExitDelete(carouselKey, s.RedisConnection)
			// getting carousel
			for _, carouselValues := range pageValue.Carousel {

				carobj := pb.Carousel{
					ImageUrl:    carouselValues.ImageUrl,
					Target:      carouselValues.Target,
					Title:       carouselValues.Title,
					PackageName: carouselValues.PackageName,
				}

				resultByteArray, err := proto.Marshal(&carobj)
				if err != nil {
					return  err
				}

				// setting page carousel in redis
				result := s.RedisConnection.SAdd(carouselKey, resultByteArray)

				if result.Err() != nil {
					log.Println(result.Err())
				}
			}

			pageObj.CarouselEndpoint = fmt.Sprintf("/carousel/%s/%s/%s", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName))
		}

		var rowPathSet []string

		// looping rows
		for _, rowValues := range pageValue.GetRow() {

			rowKey := fmt.Sprintf("%s:%s:%s:%s:%s", formatString(schedule.Vendor),
				formatString(schedule.Brand),
				formatString(pageValue.PageName),
				formatString(rowValues.RowName),
				formatString(rowValues.RowType.String()))

			rowPathSet = append(rowPathSet, fmt.Sprintf("/row/%s/%s/%s/%s/%s", 	formatString(schedule.GetVendor()),
				formatString(schedule.GetBrand()),
				formatString(pageValue.GetPageName()),
				formatString(rowValues.GetRowName()),
				formatString(rowValues.GetRowType().String())))


			ifExitDelete(rowKey, s.RedisConnection)
			// making stages
			pipeline := pipelineMaker(rowValues)

			// creating aggregation query
			tileCur, err := s.TileCollection.Aggregate(context.Background(), pipeline)
			if err != nil {
				log.Println(err)
			}

			defer tileCur.Close(ctx)

			contentkey := fmt.Sprintf("%s:content", rowKey)
			ifExitDelete(contentkey, s.RedisConnection)

			for tileCur.Next(ctx) {
				var contentTile pb.ContentTile
				if rowValues.RowType == pb.RowType_Editorial {
					var temp EditorialTemp
					err = tileCur.Decode(&temp)
					if err != nil {
						return  err
					}
					contentTile.ContentId = temp.ContentID
					contentTile.IsDetailPage = temp.IsDetailPage
					contentTile.PackageName = temp.PackageName
					if len(temp.Poster) > 0 {
						contentTile.Poster = temp.Poster[0]
					}
					if len(temp.Portrait) > 0 {
						contentTile.Portrait = temp.Portrait[0]
					}
					contentTile.Target = temp.Target
					contentTile.Title = temp.Title
					contentTile.TileType = pb.TileType_ImageTile

					if contentTile.XXX_Size() > 0 {
						contentByte, err := proto.Marshal(&contentTile)
						if err != nil {
							return  err
						}
						for i, v := range rowValues.RowTileIds {
							if v == contentTile.ContentId {
								if err = s.RedisConnection.ZAdd(contentkey, redis.Z{
									Score:  float64(i),
									Member: contentByte,
								}).Err() ; err != nil {
									return err
								}
								break
							}
						}
					}

				} else  {
					var temp Temp
					err = tileCur.Decode(&temp)
					if err != nil {
						return  err
					}
					if len(temp.ContentTile) > 0 {
						contentTile.ContentId = temp.ContentTile[0].ContentID
						contentTile.IsDetailPage = temp.ContentTile[0].IsDetailPage
						contentTile.PackageName = temp.ContentTile[0].PackageName
						if len(temp.ContentTile[0].Poster) > 0 {
							contentTile.Poster = temp.ContentTile[0].Poster[0]
						}
						if len(temp.ContentTile[0].Portrait) > 0 {
							contentTile.Portrait = temp.ContentTile[0].Portrait[0]
						}
						contentTile.Target = temp.ContentTile[0].Target
						contentTile.Title = temp.ContentTile[0].Title
						contentTile.TileType = pb.TileType_ImageTile
					}

					if contentTile.XXX_Size() > 0 {
						contentByte, err := proto.Marshal(&contentTile)
						if err != nil {
							return  err
						}

						if err = s.RedisConnection.SAdd(contentkey,contentByte).Err(); err != nil {
							return err
						}
					}
				}
			}

			helperRow := pb.Row{
				RowName:     rowValues.RowName,
				RowLayout:   rowValues.Rowlayout,
				ContentBaseUrl:  "http://cloudwalker-assets-prod.s3.ap-south-1.amazonaws.com/images/tiles/",
				ContentId: contentkey,
				Shuffle:   rowValues.Shuffle,
			}
			resultByteArray, err := proto.Marshal(&helperRow)
			if err != nil {
				return  err
			}
			//TODO add base Url to it
			s.RedisConnection.SAdd(rowKey, resultByteArray)
		}

		//TODO Pages storing to redis...
		pageObj.RowContentEndpoint = rowPathSet

		resultByteArray, err := proto.Marshal(&pageObj)
		if err != nil {
			return  err
		}

		s.RedisConnection.SAdd(pageKey, resultByteArray)

		primePageObj := pb.PrimePage{
			PageName: pageValue.PageName,
			PageEndpoint: fmt.Sprintf("/page/%s/%s/%s", 			formatString(schedule.Vendor),
																		formatString(schedule.Brand),
																		formatString(pageValue.PageName)),
		}
		resultByteArray, err = proto.Marshal(&primePageObj)
		if err != nil {
			return  err
		}

		//setting prime pages in redis
		result := s.RedisConnection.SAdd(primeKey, resultByteArray)
		if result.Err() != nil {
			return result.Err()
		}
	}
	return nil
}

