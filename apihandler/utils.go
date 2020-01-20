package apihandler

import (
	pb "Cw_Schedule/proto"
	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)


// helper structs
// for Editorial Type
type EditorialTemp struct {
	ID struct {
		Oid string `json:"$oid"`
	} `json:"_id"`
	Title        string   `json:"title"`
	Portrait     []string `json:"portrait"`
	Poster       []string `json:"poster"`
	ContentID    string   `json:"contentId"`
	IsDetailPage bool     `json:"isDetailPage"`
	PackageName  string   `json:"packageName"`
	Target       []string `json:"target"`
}


// for dynamic Row type
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
		Created_at       string `json:"created_at"`
		Updated_at       string `json:"updated_at"`
	} `json:"contentTile"`
}


// helper function
func formatString(value string) string {
	return strings.ToLower(strings.Replace(value, " ", "_", -1))
}

func pipelineMaker(rowValues *pb.ScheduleRow) mongo.Pipeline {
	// creating pipes for mongo aggregation
	pipeline := mongo.Pipeline{}

	//pipeline = append(pipeline , bson.D{{"$match", bson.D{{"content.publishState", true}}}})

	if rowValues.GetRowType() == pb.RowType_Editorial {
		// Adding stages 1
		pipeline = append(pipeline, bson.D{{"$match", bson.D{{"ref_id", bson.D{{"$in", rowValues.GetRowTileIds()}}}}}})

		//Adding stage 2
		pipeline = append(pipeline, bson.D{{"$project", bson.D{
			{"title", "$metadata.title"},
			{"portrait", "$posters.portrait",},
			{"poster", "$posters.landscape"},
			{"contentId", "$ref_id"},
			{"isDetailPage", "$content.detailPage"},
			{"packageName", "$content.package"},
			{"target", "$content.target"},}}})

	} else {
		var filterArray []bson.E
		for key, value := range rowValues.GetRowFilters() {
			if value.GetValues() != nil &&  len(value.GetValues()) > 0 {
				filterArray = append(filterArray, bson.E{key, bson.D{{"$in", value.GetValues()}}})
			}
		}
		// Adding stages 1
		pipeline = append(pipeline, bson.D{{"$match", filterArray}})

		// making stage 2
		stage2 := bson.D{{"$group", bson.D{{"_id", bson.D{
			{"created_at", "$created_at"},
			{"updated_at", "$updated_at"},
			{"contentId", "$ref_id"},
			{"releaseDate", "$metadata.releaseDate"},
			{"year", "$metadata.year"},
			{"imdbid", "$metadata.imdbid"},
			{"rating", "$metadata.rating"},
			{"viewCount", "$metadata.viewCount"},

		}}, {"contentTile", bson.D{{"$push", bson.D{
			{"title", "$metadata.title"},
			{"portrait", "$posters.portrait",},
			{"poster", "$posters.landscape"},
			{"contentId", "$ref_id"},
			{"isDetailPage", "$content.detailPage"},
			{"packageName", "$content.package"},
			{"target", "$content.target"},
			{"releaseDate", "$metadata.releaseDate"},
			{"year", "$metadata.year"},
		}}}}}}}

		pipeline = append(pipeline, stage2)

		if rowValues.GetRowSort() != nil {
			// making stage 3
			var sortArray []bson.E
			for key, value := range rowValues.GetRowSort() {
				sortArray = append(sortArray, bson.E{strings.TrimSpace(strings.Replace(key, "metadata", "_id", -1 )), value})
			}
			//stage 3
			stage3 := bson.D{{"$sort", sortArray}}
			pipeline = append(pipeline, stage3)
		}
	}

	return pipeline
}

func ifExitDelete(redisKey string, redisConn *redis.Client )  {
	if redisConn.Exists(redisKey).Val() ==  1 {
		redisConn.Del(redisKey)
	}
}