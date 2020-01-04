package main

import (
	"Cw_Schedule/apihandler"
	pb "Cw_Schedule/proto"
	"context"
	"fmt"
	codecs "github.com/amsokol/mongo-go-driver-protobuf"
	"github.com/go-redis/redis"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	defaultHost          = "mongodb://nayan:tlwn722n@cluster0-shard-00-00-8aov2.mongodb.net:27017,cluster0-shard-00-01-8aov2.mongodb.net:27017,cluster0-shard-00-02-8aov2.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority"
	developmentMongoHost = "mongodb://192.168.1.9:27017"
	schedularMongoHost   = "mongodb://192.168.1.143:27017"
	schedularRedisHost   = "192.168.1.143:6379"
)

// private type for Context keys
type contextKey int

const (
	clientIDKey contextKey = iota
)

var scheduleCollection, tileCollection *mongo.Collection
var tileRedis *redis.Client

// Multiple init() function
func init() {
	fmt.Println("Welcome to init() function")
	scheduleCollection = getMongoCollection("cloudwalker", "schedule", defaultHost)
	tileCollection = getMongoCollection("cwtx2devel", "tiles", developmentMongoHost)
	tileRedis = getRedisClient(schedularRedisHost)
}

func credMatcher(headerName string) (mdName string, ok bool) {
	if headerName == "Login" || headerName == "Password" {
		return headerName, true
	}
	return "", false
}

// authenticateAgent check the client credentials
func authenticateClient(ctx context.Context, s *apihandler.Server) (string, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		clientLogin := strings.Join(md["login"], "")
		clientPassword := strings.Join(md["password"], "")
		if clientLogin != "nayan" {
			return "", fmt.Errorf("unknown user %s", clientLogin)
		}
		if clientPassword != "makasare" {
			return "", fmt.Errorf("bad password %s", clientPassword)
		}
		log.Printf("authenticated client: %s", clientLogin)
		return "42", nil
	}
	return "", fmt.Errorf("missing credentials")
}

func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	s, ok := info.Server.(*apihandler.Server)
	if !ok {
		return nil, fmt.Errorf("unable to cast the server")
	}
	clientID, err := authenticateClient(ctx, s)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, clientIDKey, clientID)
	return handler(ctx, req)
}

func startGRPCServer(address, certFile, keyFile string) error {
	// create a listener on TCP port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	} // create a server instance
	s := apihandler.Server{
		scheduleCollection,
		tileRedis,
		tileCollection,
	}

	// Create the TLS credentials
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("could not load TLS keys: %s", err)
	} // Create an array of gRPC options with the credentials
	_ = []grpc.ServerOption{grpc.Creds(creds), grpc.UnaryInterceptor(unaryInterceptor)}

	// create a gRPC server object
	//grpcServer := grpc.NewServer(opts...)

	// attach the Ping service to the server
	grpcServer := grpc.NewServer()                    // attach the Ping service to the server
	pb.RegisterSchedularServiceServer(grpcServer, &s) // start the server
	log.Printf("starting HTTP/2 gRPC server on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %s", err)
	}
	return nil
}

func startRESTServer(address, grpcAddress, certFile string) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(credMatcher))
	//creds, err := credentials.NewClientTLSFromFile(certFile, "")
	//if err != nil {
	//	return fmt.Errorf("could not load TLS certificate: %s", err)
	//}  // Setup the client gRPC options
	//
	//opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}  // Register ping

	opts := []grpc.DialOption{grpc.WithInsecure()} // Register ping
	err := pb.RegisterSchedularServiceHandlerFromEndpoint(ctx, mux, grpcAddress, opts)
	if err != nil {
		return fmt.Errorf("could not register service Ping: %s", err)
	}

	log.Printf("starting HTTP/1.1 REST server on %s", address)
	http.ListenAndServe(address, mux)
	return nil
}

func getMongoCollection(dbName, collectionName, mongoHost string) *mongo.Collection {
	// Register custom codecs for protobuf Timestamp and wrapper types
	reg := codecs.Register(bson.NewRegistryBuilder()).Build()
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoHost), options.Client().SetRegistry(reg))
	if err != nil {
		log.Fatal(err)
	}
	return mongoClient.Database(dbName).Collection(collectionName)
}

func getRedisClient(redisHost string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatalf("Could not connect to redis %v", err)
	}
	return client
}

func main() {
	//grpcAddress := fmt.Sprintf("%s:%d", "cloudwalker.services.tv", 7775)
	//restAddress := fmt.Sprintf("%s:%d", "cloudwalker.services.tv", 7776)

	grpcAddress := fmt.Sprintf("%s:%d", "192.168.1.143", 7775)
	restAddress := fmt.Sprintf("%s:%d", "192.168.1.143", 7776)
	certFile := "cert/server.crt"
	keyFile := "cert/server.key"

	// fire the gRPC server in a goroutine
	go func() {
		err := startGRPCServer(grpcAddress, certFile, keyFile)
		if err != nil {
			log.Fatalf("failed to start gRPC server: %s", err)
		}
	}()

	// fire the REST server in a goroutine
	go func() {
		err := startRESTServer(restAddress, grpcAddress, certFile)
		if err != nil {
			log.Fatalf("failed to start gRPC server: %s", err)
		}
	}()

	// registering a cron job
	//registeringCron()

	// infinite loop
	log.Printf("Entering infinite loop")
	select {}
}

//func registeringCron(){
//	// make and launching cron job
//	c := cron.New()
//	_, err := c.AddFunc("@every m", func() {
//		log.Println("Cron Triggered " + string(time.Now().Hour()))
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	//staring cron job
//	c.Start()
//	//stop cron job on exit
//	defer c.Stop()
//}

//func revampingSchedule(){
//
//	//gettting current hour
//	hours, _, _ := time.Now().Clock()
//
//	//making filter query where we find the schedule in the time frame for eg : if current timing is 11 o'clock  and we have schedule 9 to 12 then it will be served.
//	filter := bson.M{"$and" : []bson.M{bson.M{"starttime": bson.M{"$lte" : hours}}, bson.M{"endtime" :  bson.M{"$gt" : hours}} }}
//
//	cur, err := scheduleCollection.Find(context.Background(), filter)
//
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	type primePage struct {
//		pageName string `json:"pageName"`
//		pageEndpoint string `json:"pageEndpoint"`
//	}
//
//
//	//	{
//	//		"carouselEndpoint": "home/carousel",
//	//		"rowContentEndpoint": [
//	//		"home/rowContent1",
//	//		"home/rowContent2",
//	//		"home/rowContent3",
//	//		"home/rowContent4",
//	//		"home/rowContent5"
//	//]
//	//	}
//
//
//	type Page struct {
//		carouselEndpoint string  `json:"carouselEndpoint"`
//		rowContentEndpoint []string  `json:"rowContentEndpoint"`
//	}
//
//
//	type carosuel struct{
//		imageUrl string `json:"imageUrl"`
//		target string `json:"target"`
//		title string `json:"title"`
//		packageName string `json:"packageName"`
//	}
//
//
//
//	type contentTile struct {
//		movieTitle string
//		portrait []string
//		poster []string
//		movieId string
//		isDetailPage bool
//		target []string
//		packagename string
//	}
//
//	type row struct {
//		rowName string `json:"rowName"`
//		rowLayout pb.RowLayout `json:"rowLayout"`
//		rowBaseUrl string `json:"rowBaseUrl"`
//		contentTile []contentTile `json:"contentTiles"`
//	}
//
//
//	for cur.Next(context.Background()){
//
//		// getting schedule
//		var schedule pb.Schedule
//		err = cur.Decode(&schedule)
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		primePages := make([]*primePage, len(schedule.Pages))
//
//		// getting pages
//		for _, pageValue := range schedule.Pages {
//
//
//			carouselKey := fmt.Sprintf("%s:%s:%s:carousel",strings.ToLower(schedule.Vendor), strings.ToLower(schedule.Brand), strings.ToLower(strings.Replace(pageValue.PageName, " ", "_", -1)))
//			rowKey := fmt.Sprintf("%s:%s:%s:rows",strings.ToLower(schedule.Vendor), strings.ToLower(schedule.Brand), strings.ToLower(strings.Replace(pageValue.PageName, " ", "_", -1)))
//
//
//
//			// getting carousel
//			for _, carouselValues := range pageValue.Carousel {
//				// setting page carousel in redis
//				result := tileRedis.SAdd(carouselKey ,  &carosuel{
//					imageUrl:    carouselValues.ImageUrl,
//					target:      carouselValues.Target,
//					title:       carouselValues.Title,
//					packageName: carouselValues.PackageName,
//				})
//
//				if result.Err() != nil {
//					log.Fatal(result.Err())
//				}
//			}
//
//			// creating pipes for mongo aggregation
//			myStages := mongo.Pipeline{}
//
//			// getting rows
//			for _, rowValues := range pageValue.GetRow() {
//				if rowValues.RowType == pb.RowType_Editorial {
//					myStages = append(myStages, bson.D{{"$match", bson.D{{"ref_id", bson.D{{"$in", rowValues.RowTileIds}}}}}})
//				}else {
//					for key, value := range rowValues.RowFilters {
//						// Adding stages
//						myStages = append(myStages, bson.D{{"$match", bson.D{{key, bson.D{{"$in", value.Values}}}}}})
//					}
//					myStages = append(myStages,bson.D{{"$sort", bson.D{{"created_at", -1}, {"updated_at", -1}, {"metadata.year", -1}}}},)
//				}
//				myStages = append( myStages , bson.D{{"$project", bson.D{
//					{"_id", 0},
//					{"contentId", "ref_id"},
//					{"title", "$metadata.title"},
//					{"poster", "posters.landscape"},
//					{"portrait", "posters.portrait"},
//					{"packageName", "content.package"},
//					{"target", "content.target"},
//					{"isDetailPage", "content.detailPage"},
//					{"realeaseDate", "metadata.releaseDate"},
//				}}} )
//
//				// creating aggregation query
//				tileCur, err := tileCollection.Aggregate(context.Background(), myStages)
//				if(err != nil){
//					log.Fatal(err)
//				}
//
//				var contentTiles []contentTile
//
//				for tileCur.Next(context.Background()){
//					var contentTile contentTile
//					err = cur.Decode(&contentTile)
//					if err != nil {
//						log.Fatal(err)
//					}
//					contentTiles = append(contentTiles, contentTile)
//
//				}
//
//
//				//TODO add base Url to it
//				tileRedis.SAdd(rowKey, &row{
//					rowName:    rowValues.RowName,
//					rowLayout:  rowValues.Rowlayout,
//				})
//
//			}
//
//			// appending to the slice
//			primePages = append(primePages, &primePage{
//				pageName:     pageValue.PageName,
//				pageEndpoint: strings.ToLower(strings.Replace(pageValue.PageName, " ", "_", -1)),
//			})
//		}
//
//
//		//setting prime pages in redis
//		result := tileRedis.Set(fmt.Sprintf("%s:%s:cloudwalkerPrimePages", strings.ToLower(schedule.Vendor), strings.ToLower(schedule.Brand)), primePages, 3 * time.Hour)
//		if result.Err() != nil {
//			log.Fatal(result.Err())
//		}
//	}
//}
