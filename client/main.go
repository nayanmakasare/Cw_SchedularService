package main

import (
	pb "Cw_Schedule/proto"
	"context"
	"google.golang.org/grpc"
	"io"
	"log"
	"sync"
)

type Authentication struct {
	Login    string
	Password string
}

func (a *Authentication) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"login":    a.Login,
		"password": a.Password,
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires transport security
func (a *Authentication) RequireTransportSecurity() bool {
	return true
}

func makingConnection() (*grpc.ClientConn, error) {
	// creating creds for grpc client connection
	//creds, err := credentials.NewClientTLSFromFile("cert/server.crt", "")
	//if err != nil {
	//	log.Fatalf("could not load tls cert: %s", err)
	//}
	//
	//auth := Authentication{
	//	Login:    "nayan",
	//	Password: "makasare",
	//}

	//conn , err := grpc.Dial("192.168.1.143:7775", grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(&auth))
	conn, err := grpc.Dial("192.168.1.143:7775", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	return conn, err
}

type primePage struct {
	pageName     string `json:"pageName"`
	pageEndpoint string `json:"pageEndpoint"`
}

func main() {
	// stress testing run the rpc for how many times
	run_test_index := 1

	var wg sync.WaitGroup
	conn, err := makingConnection()
	if err != nil {
		log.Fatal(err)
	}
	c := pb.NewSchedularServiceClient(conn)

	wg.Add(run_test_index)
	for i := 0; i < run_test_index; i++ {
		//go CreateSchedule(c, &wg)
		go RefreshSchedule(c, &wg)
		//go GetScheduler(c, &wg)
		//go UpdateSchedule(c, &wg)
		//go DeleteSchedule(c, &wg)
	}
	wg.Wait()
}


func DeleteSchedule(c pb.SchedularServiceClient, wg *sync.WaitGroup) {
	resp, err := c.DeleteSchedule(context.Background(), &pb.DeleteScheduleRequest{Vendor:"cvte", Brand:"shinko"})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(resp.IsSuccessful)
	wg.Done()
}


func RefreshSchedule(c pb.SchedularServiceClient, wg *sync.WaitGroup) {
	result, err := c.RefreshSchedule(context.Background(), &pb.RefreshScheduleRequest{
		Vendor:               "cvte",
		Brand:                "shinko",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	wg.Done()
}

func CreateSchedule(c pb.SchedularServiceClient, wg *sync.WaitGroup) {

	//Row Filter 1
	rowFilter11 := make(map[string]*pb.RowFilterValue)
	rowFiltValue111 := pb.RowFilterValue{
		Values: []string{"Movie Trailers"},
	}
	rowFilter11["metadata.categories"] = &rowFiltValue111

	rowFiltValue112 := pb.RowFilterValue{
		Values: []string{"English", "Hindi"},
	}
	rowFilter11["metadata.languages"] = &rowFiltValue112

	//Row filter 2
	rowFilter12 := make(map[string]*pb.RowFilterValue)

	rowFiltValue122 := pb.RowFilterValue{
		Values: []string{"Hindi", "Hindi Dub"},
	}
	rowFilter12["metadata.languages"] = &rowFiltValue122

	rowFiltValue123 := pb.RowFilterValue{
		Values: []string{"Hindi Dub Movies"},
	}
	rowFilter12["metadata.categories"] = &rowFiltValue123

	// row sort 1
	rowSort11 := make(map[string]int32)

	rowSort11["created_at"] = -1
	rowSort11["updated_at"] = -1

	// Rows in Page 1
	rows1 := []*pb.ScheduleRow{
		{
			Rowlayout:  pb.RowLayout_Landscape,
			RowName:    "Trailers",
			RowIndex:   0,
			RowFilters: rowFilter11,
			RowTileIds: nil,
			RowSort:    rowSort11,
			RowType:    pb.RowType_Dynamic,
		},
		{
			Rowlayout:  0,
			RowName:    "Hindi Dub Movies",
			RowIndex:   0,
			RowFilters: rowFilter12,
			RowTileIds: nil,
			RowSort:    rowSort11,
			RowType:    pb.RowType_Dynamic,
			Shuffle:    true,
		},
	}

	carouesl1 := []*pb.ScheduleCarousel{
		{
			Target:      "111111",
			PackageName: "in.startv.hotstar",
			ImageUrl:    "82513053a4b2d84790cb5a4f436b2371/6Underground_1920x500.webp",
			Title:       "title 1",
		},
		{
			Target:      "222222",
			PackageName: "in.startv.hotstar",
			ImageUrl:    "82513053a4b2d84790cb5a4f436b2371/6Underground_1920x500.webp",
			Title:       "title 2",
		},
		{
			Target:      "333333",
			PackageName: "in.startv.hotstar",
			ImageUrl:    "82513053a4b2d84790cb5a4f436b2371/6Underground_1920x500.webp",
			Title:       "title 3",
		},
	}

	pages := []*pb.SchedulePage{
		{
			PageName:  "Movies",
			PageIndex: 0,
			PageLogo:  "",
			Row:       rows1,
			Carousel:  carouesl1,
		},
		{
			PageName:  "Kids",
			PageIndex: 1,
			PageLogo:  "",
			Row:       rows1,
			Carousel:  carouesl1,
		},
		{
			PageName:  "Music",
			PageIndex: 2,
			PageLogo:  "",
			Row:       rows1,
			Carousel:  carouesl1,
		},
	}

	vbs := &pb.Schedule{
		Brand:     "shinko",
		Vendor:    "cvte",
		StartTime: 12,
		EndTime:   15,
		Pages:     pages,
		BaseUrl: "http://cloudwalker-assets-prod.s3.ap-south-1.amazonaws.com/images/tiles/",
	}

	resp, err := c.CreateSchedule(context.Background(), vbs)
	if err != nil {
		log.Fatalf("error when calling RegisterBrand: %s", err)
	}
	log.Println(resp)
	wg.Done()
}

func UpdateSchedule(c pb.SchedularServiceClient, wg *sync.WaitGroup) {
	vbs := &pb.Schedule{
		Brand:     "shinko",
		Vendor:    "cvte",
		StartTime: 12,
		EndTime:   15,
		Pages:     []*pb.SchedulePage{},
	}

	resp, err := c.UpdateSchedule(context.Background(), vbs)
	if err != nil {
		log.Fatalf("error when calling RegisterBrand: %s", err)
	}
	log.Println(resp)
	wg.Done()
}

func GetScheduler(c pb.SchedularServiceClient, wg *sync.WaitGroup) {
	resp, err := c.GetSchedule(context.Background(), &pb.GetScheduleRequest{
		Vendor: "cvte",
		Brand:  "shinko",
	})
	if err != nil {
		log.Fatalf("error when calling RegisterBrand: %s", err)
	}

	for {
		vbs, err := resp.Recv()
		if err == io.EOF {
			resp.CloseSend()
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Println(vbs)
	}
	wg.Done()
}
