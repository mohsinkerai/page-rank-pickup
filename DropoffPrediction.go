package main

import (
	"github.com/cdipaolo/goml/cluster"
	"github.com/alixaxel/pagerank"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"log"
	"database/sql"
	"math/rand"
	"sort"
	"github.com/remeh/sizedwaitgroup"
	"time"
)

type LatLng struct {
	Lat, Lng float64
}

type TripData struct {
	pickup, dropoff LatLng
	id uint64
	pickupCluster, dropoffCluster uint8
	pickupTime, dropoffTime time.Time
	cost float64
	taxiId uint64
}

type ClusterRank struct {
	clusterId int
	rank float64
}

//func main() {
//	isWeekendValues := 2
//	hours := 24
//	kMeansK := 50
//	kMeansMaxIteration := 1000
//	kFold := 10
//
//	swg := sizedwaitgroup.New(4)
//
//	for i:= 0; i < isWeekendValues-1; i++ {
//		for j:=22; j < hours; j++ {
//			swg.Add()
//			go runLogic(kFold, j, i, kMeansK, kMeansMaxIteration, 10, &swg)
//		}
//	}
//	swg.Wait()
//}

func runLogic(kFold int, j int, i int, kMeansK int, kMeansMaxIteration int, bucketCount int,wg *sizedwaitgroup.SizedWaitGroup) {
	defer wg.Done()
	var resultingBucket []uint64
	for i := 0; i < bucketCount; i++ {
		resultingBucket = append(resultingBucket, uint64(0))
	}
	for k := 0; k <= kFold; k++ {
		var trainTrips []TripData;
		var testTrips []TripData;
		for l := 0; l < kFold; l++ {
			if l == k {
				testTrips = loadTripsForSpecificFold(l+1, j, i);
			} else {
				trainTrips = append(trainTrips, loadTripsForSpecificFold(l+1, j, i)...)
			}
		}
		rankedClusters, kMeansModel := train(trainTrips, kMeansK, kMeansMaxIteration)
		clusterRank := convertRank(rankedClusters, kMeansK)
		buckets := test(testTrips, kMeansModel, clusterRank, bucketCount)

		//log.Println("Logging for Hour", j, "isWeekend", i, "Fold", k)
		//log.Println("Top 1 Got Guess", buckets[0], "Top 2", buckets[1], "Top 3", buckets[2], "Top 4", buckets[3], "Top 5", buckets[4])
		//log.Println("Rest Bucket", buckets[5])
		for i := 0; i < bucketCount; i++ {
			resultingBucket[i] += buckets[i]
		}
	}
	total := uint64(0)
	for _, i := range resultingBucket {
		total += i
	}
	for count, bucket := range resultingBucket {
		log.Println("For IsWeekend", i, "Hour", j, "Bucket", count, "Contains % of Correctness", bucket, float64(bucket)*100/float64(total))
	}
}

func train(trainTrips []TripData, kMeansK int, kMeansMaxIteration int) ([]float64, *cluster.KMeans){
	model := trainKMeans(trainTrips, kMeansK, kMeansMaxIteration)
	trainTrips = predictClusterAndAssign(trainTrips, model)
	frequencyMap := generateClusterFrequencyMap(trainTrips, kMeansK)
	rankedClusters := applyPageRank(frequencyMap, kMeansK)
	return rankedClusters, model
}

func convertRank(ranks []float64, kMeansK int) []ClusterRank {
	var clusterRanks []ClusterRank
	for i:=0; i<kMeansK; i++ {
		clusterRanks = append(clusterRanks, ClusterRank{i,ranks[i]})
	}
	return clusterRanks
}

func test(testTrips []TripData, model *cluster.KMeans, rankedClusters []ClusterRank, bucketCount int) []uint64{
	var buckets []uint64

	for i:=0;i<bucketCount;i++ {
		buckets = append(buckets, 0)
	}

	testTrips = predictClusterAndAssign(testTrips, model)
	cpy := make([]ClusterRank, len(rankedClusters))
	copy(cpy, rankedClusters)
	sort.Slice(cpy, func(i,j int) bool {
		return rankedClusters[i].rank > rankedClusters[j].rank
	})
	log.Println(cpy)
	for _,trip := range testTrips {
		dropOffCluster:=trip.dropoffCluster
		isAssigned := false
		for i:=0;i<bucketCount-1;i++ {
			if uint8(cpy[i].clusterId) == dropOffCluster {
				isAssigned = true
				buckets[i]++
				break
			}
		}
		if isAssigned == false {
			buckets[bucketCount-1] ++
		}
	}
	return buckets;
}

func generateClusterFrequencyMap(trips []TripData, kMeansK int) [][]float64 {
	var frequencyMap [][]float64

	for i:= 0; i < kMeansK; i++ {
		var fmap []float64
		for j:= 0; j < kMeansK; j++ {
			fmap = append(fmap, 0)
		}
		frequencyMap = append(frequencyMap, fmap)
	}

	for _, trip := range trips {
		frequencyMap[trip.pickupCluster][trip.dropoffCluster]++
	}

	return frequencyMap
}

func predictClusterAndAssign(trips []TripData, result *cluster.KMeans) []TripData{
	for idx, trip := range trips {
		//log.Println(reflect.TypeOf(trip))
		pred, err := result.Predict([]float64{trip.pickup.Lat, trip.pickup.Lng})
		if err != nil {
			panic("prediction error")
		}
		trips[idx].pickupCluster = uint8(pred[0])
		//log.Println(trip.pickupCluster)
		pred, err = result.Predict([]float64{trip.dropoff.Lat, trip.dropoff.Lng})
		if err != nil {
			panic("prediction error")
		}
		trips[idx].dropoffCluster = uint8(pred[0])
		//log.Println(trip.dropoffCluster)
	}
	return trips
}

func applyPageRank(frequencyMap [][]float64, kMeansK int) []float64{
	graph := pagerank.NewGraph()
	var rankedCluster []float64
	//log.Println("Frequency Map", frequencyMap)

	for i:= 0; i<kMeansK; i++ {
		for j:= 0; j < kMeansK; j++ {
			graph.Link(uint32(i), uint32(j), float64(frequencyMap[i][j]))
		}
	}

	for i:= 0; i<kMeansK; i++ {
		rankedCluster = append(rankedCluster, 0)
	}

	graph.Rank(0.85, 0.000001, func(node uint32, rank float64) {
		rankedCluster[node] = rank
		//fmt.Println("Node", node, "has a rank of", rank)
	})
	return rankedCluster
}

func trainKMeans(trips []TripData, kValue int, maxIterations int) *cluster.KMeans{

	double := [][]float64{}

	for _,trip := range trips {
		double = append(double, []float64{trip.pickup.Lat, trip.pickup.Lng})
		double = append(double, []float64{trip.dropoff.Lat, trip.dropoff.Lng})
	}

	model := cluster.NewKMeans(kValue, maxIterations, double)
	if model.Learn() != nil {
		panic("Oh NO!!! There was an error learning!!")
	}

	fmt.Println(model)
	return model
}

/**
Function is final and tested
 */
func loadTripsForSpecificFold(foldId int, hour int, weekdayWeekend int) []TripData{

	var trips []TripData

	db, err := sql.Open("mysql", "root:123@/thesis")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	query, err := db.Prepare("SELECT t.id, t.lat_pickup, t.lng_pickup, t.lat_dropoff, t.lng_dropoff from trip t inner join date_dimention dd on t.date_dimention_id = dd.id where t.k_fold_id = ? and t.lat_pickup is not null and dd.hour = ? and dd.is_weekend = ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	rows, err := query.Query(foldId, hour, weekdayWeekend)
	if err != nil {
		panic (err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var pickup, dropoff LatLng
		var trip TripData
		err := rows.Scan(&trip.id, &pickup.Lat, &pickup.Lng, &dropoff.Lat, &dropoff.Lng)
		if err != nil {
			log.Println(rows)
			log.Fatal(err)
		}
		trip.pickup = pickup
		trip.dropoff = dropoff
		trips = append(trips, trip)
	}
	return trips;
}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

/**
Used to populate K Fold Id
 */
func populateKFoldIdInDB(start, end int) {
	db, err := sql.Open("mysql", "root:123@/thesis")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	query, err := db.Prepare("UPDATE trip SET k_fold_id=? where id = ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	for i:=start; i<end; i++ {
		query.Exec(rand.Intn(10)+1, i)
	}
}