package main

import (
	"log"
	"database/sql"
	"github.com/cdipaolo/goml/cluster"
	"sort"
	"github.com/remeh/sizedwaitgroup"
)

func main3() {
	maxFold := 10
	//fold := 1
	kMeansK := 100
	kMeansMaxIteration := 1000
	bucketCount:= 10

	log.Println("kMeansK", kMeansK, "kMeansMaxIteration", kMeansMaxIteration, "bucketCount", bucketCount)

	swg := sizedwaitgroup.New(5)

	for fold:=0;fold<maxFold;fold++ {
		db, err := sql.Open("mysql", "root:123@/thesis")
		if err != nil {
			panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
		}
		defer db.Close()
		swg.Add()
		go runLogicForPickup(fold+1, db, kMeansK, kMeansMaxIteration,bucketCount,&swg)
	}
	swg.Wait()

}

func runLogicForPickup(fold int, db *sql.DB, kMeansK int, kMeansMaxIteration int, bucketCount int, wg *sizedwaitgroup.SizedWaitGroup) {
	taxiIds := getFoldTaxiNegateIds(fold, db)
	testTaxiIds := getFoldTaxiIds(fold, db)
	rankedClusters, model := train_pickup(taxiIds, kMeansK, kMeansMaxIteration)
	clusterRank := convertRank(rankedClusters, kMeansK)
	continueCount, breakTrips, tripTimeForPredictionFollowers, tripTimeForPredictionNonFollowers := test_pickup(testTaxiIds, model, clusterRank, bucketCount, fold, db)
	log.Println("ContinueCount ", continueCount, "breakTrips", breakTrips, "Positive", average(tripTimeForPredictionFollowers), "+veTotal", len(tripTimeForPredictionFollowers), "Negative", average(tripTimeForPredictionNonFollowers), "-veTotal", len(tripTimeForPredictionNonFollowers))
	wg.Done()
}

func average(arr []int) float64 {
	var sum int64
	sum = 0
	if len(arr) == 0 {
		return 0
	}
	for _,element := range arr {
		sum += int64(element)
	}
	return float64(sum)/float64(len(arr))
}

/**
Training Model with Reverse Frequency Map
 */
func test_pickup(taxiIds []uint64, model *cluster.KMeans, rankedClusters []ClusterRank, bucketCount int, fold int, db *sql.DB) (int, int, []int, []int){
	continueCount := 0
	breakTrips := 0

	var tripCollection []TripData
	tripCollection = loadTripsForTaxis(taxiIds)
	tripCount := len(tripCollection)

	tripCollection = predictClusterAndAssign(tripCollection, model)
	// Now cpy contains sorted on rank
	cpy := make([]ClusterRank, len(rankedClusters))
	copy(cpy, rankedClusters)
	sort.Slice(cpy, func(i,j int) bool {
		return rankedClusters[i].rank > rankedClusters[j].rank
	})
	var tripTimeForPredictionFollowers []int
	var tripTimeForPredictionNonFollowers []int

	//log.Println(cpy)
	for i,trip := range tripCollection {
		if(i >= tripCount-1) {
			continueCount++
			continue
		}

		nextTrip := tripCollection[i+1]
		currentTrip := trip

		if (currentTrip.taxiId != nextTrip.taxiId) {
			continueCount++
			continue
		}

		if(nextTrip.pickupTime.Sub(currentTrip.dropoffTime).Minutes() >= 30) {
			breakTrips++
			continue
		} else {
			isAssigned := false
			for j:= 0;j<bucketCount;j++ {
				if uint8(cpy[j].clusterId) == nextTrip.pickupCluster {
					// Add to Bucket
					isAssigned = true
					nextTripTime := int(nextTrip.dropoffTime.Sub(nextTrip.pickupTime).Seconds())
					tripTimeForPredictionFollowers = append(tripTimeForPredictionFollowers, nextTripTime)
					store_results(nextTrip.id, len(model.Centroids), fold, nextTrip.taxiId, 1, nextTripTime, j+1, db)
				}
			}
			if isAssigned == false {
				nextTripTime := int(nextTrip.dropoffTime.Sub(nextTrip.pickupTime).Seconds())
				tripTimeForPredictionNonFollowers = append(tripTimeForPredictionNonFollowers, nextTripTime)
				store_results(nextTrip.id, len(model.Centroids), fold, nextTrip.taxiId, 0, nextTripTime, 0, db)
			}
			// next pickup in top3, good
			// else false
		}
	}
	return continueCount, breakTrips, tripTimeForPredictionFollowers, tripTimeForPredictionNonFollowers;
}

/**
Training Model with Reverse Frequency Map
 */
func train_pickup(taxiIds []uint64, kMeansK int, kMeansMaxIteration int) ([]float64, *cluster.KMeans){
	var tripCollection []TripData
	tripCollection = loadTripsForTaxis(taxiIds)
	model := trainKMeans(tripCollection, kMeansK, kMeansMaxIteration)
	tripCollection = predictClusterAndAssign(tripCollection, model)
	frequencyMap := generateReverseClusterFrequencyMap(tripCollection, kMeansK)
	rankedClusters := applyPageRank(frequencyMap, kMeansK)
	return rankedClusters, model
}

func generateReverseClusterFrequencyMap(trips []TripData, kMeansK int) [][]float64 {
	var frequencyMap [][]float64

	for i:= 0; i < kMeansK; i++ {
		var fmap []float64
		for j:= 0; j < kMeansK; j++ {
			fmap = append(fmap, 0)
		}
		frequencyMap = append(frequencyMap, fmap)
	}

	for _, trip := range trips {
		frequencyMap[trip.dropoffCluster][trip.pickupCluster]++
	}

	return frequencyMap
}

/**
Function is final and tested
 */
func loadTripsForTaxis(taxiIds []uint64) []TripData{

	var trips []TripData

	db, err := sql.Open("mysql", "root:123@/thesis?parseTime=true")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	queryString := "SELECT t.id, t.lat_pickup, t.lng_pickup, t.lat_dropoff, t.lng_dropoff, t.taxi_id , t.timestamp, t.trip_end_timestamp, t.trip_cost " +
		"FROM trip t " +
		"WHERE t.taxi_id = ? " +
		"AND t.lat_pickup is not null " +
		"AND t.trip_distance >= 1000 " +
		"ORDER BY t.taxi_id, t.timestamp"
		//log.Println(queryString)

	query, err := db.Prepare(queryString)
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	//log.Println("Query Param will be", queryParam)
	for _, taxiId := range taxiIds {
		//log.Println("Doing query for ", taxiId)
		rows, err := query.Query(taxiId)
		if err != nil {
			panic(err.Error())
		}
		defer rows.Close()

		for rows.Next() {
			var trip TripData
			err := rows.Scan(&trip.id, &trip.pickup.Lat, &trip.pickup.Lng, &trip.dropoff.Lat, &trip.dropoff.Lng, &trip.taxiId, &trip.pickupTime, &trip.dropoffTime, &trip.cost)
			//log.Println(trip.dropoffTime)
			if err != nil {
				log.Println(rows)
				log.Fatal(err)
			}
			trips = append(trips, trip)
		}
	}
	log.Println("Done Loading ", len(taxiIds), "Taxis and ", len(trips), "Trips")
	return trips;
}

/**
Function is final and tested
 */
func getFoldTaxiIds(fold int, db *sql.DB) []uint64{

	var taxiIds []uint64

	query, err := db.Prepare("SELECT distinct  t.taxi_id from trip t where k_fold_driver_id = ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	rows, err := query.Query(fold)
	if err != nil {
		panic (err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var taxiId uint64
		err := rows.Scan(&taxiId)
		if err != nil {
			log.Println(rows)
			log.Fatal(err)
		}
		taxiIds = append(taxiIds, taxiId)
	}
	return taxiIds;
}

/**
Function is final and tested
 */
func getFoldTaxiNegateIds(fold int, db *sql.DB) []uint64{

	var taxiIds []uint64

	query, err := db.Prepare("SELECT distinct t.taxi_id from trip t where k_fold_driver_id != ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	rows, err := query.Query(fold)
	if err != nil {
		panic (err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var taxiId uint64
		err := rows.Scan(&taxiId)
		if err != nil {
			log.Println(rows)
			log.Fatal(err)
		}
		taxiIds = append(taxiIds, taxiId)
	}
	return taxiIds;
}

func store_results(tripId uint64, kMeansK int, fold int, taxi_id uint64, followed_prediction int, trip_time int, bucket int,  db *sql.DB) {
	query, err := db.Prepare("INSERT INTO results_pickup_pred(trip_id, k_means_k, fold, taxi_id, followed_prediction, trip_time, bucket) VALUES (?,?,?,?,?,?,?)")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	_, err = query.Exec(tripId, kMeansK, fold, taxi_id, followed_prediction, trip_time, bucket)
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()
}
