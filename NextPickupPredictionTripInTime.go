package main

import (
	"log"
	"database/sql"
	"github.com/cdipaolo/goml/cluster"
	"sort"
	"github.com/remeh/sizedwaitgroup"
	"math/big"
)

func main4() {
	maxFold := 10
	//fold := 1
	kMeansK := 50
	kMeansMaxIteration := 500
	bucketCount:= 10

	log.Println("kMeansK", kMeansK, "kMeansMaxIteration", kMeansMaxIteration, "bucketCount", bucketCount)

	swg := sizedwaitgroup.New(4)

	for fold:=0;fold<maxFold;fold++ {
		db, err := sql.Open("mysql", "root:123@/thesis")
		if err != nil {
			panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
		}
		defer db.Close()
		swg.Add()
		go runLogicForPickupInTime(fold+1, db, kMeansK, kMeansMaxIteration,bucketCount,&swg)
	}
	swg.Wait()

}

func runLogicForPickupInTime(fold int, db *sql.DB, kMeansK int, kMeansMaxIteration int, bucketCount int, wg *sizedwaitgroup.SizedWaitGroup) {
	taxiIds := getFoldTaxiNegateIdsInTime(fold, db)
	testTaxiIds := getFoldTaxiIdsInTime(fold, db)
	rankedClusters, model := train_pickupInTime(taxiIds, kMeansK, kMeansMaxIteration)
	log.Println("Training Completed for Fold", fold)
	clusterRank := convertRank(rankedClusters, kMeansK)
	continueCount, breakTrips, tripTimeForPredictionFollowers, tripTimeForPredictionNonFollowers := test_pickupInTime(testTaxiIds, model, clusterRank, bucketCount, fold, db)
	log.Println("ContinueCount ", continueCount, "breakTrips", breakTrips, "Positive", average(tripTimeForPredictionFollowers), "+veTotal", len(tripTimeForPredictionFollowers), "Negative", average(tripTimeForPredictionNonFollowers), "-veTotal", len(tripTimeForPredictionNonFollowers))
	wg.Done()
}

/**
Training Model with Reverse Frequency Map
 */
func test_pickupInTime(taxiIds []uint64, model *cluster.KMeans, rankedClusters []ClusterRank, bucketCount int, fold int, db *sql.DB) (int, int, []int, []int){
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
		freeTime := int(nextTrip.pickupTime.Sub(currentTrip.dropoffTime).Seconds());

		// 30*60 is 30 minutes
		if(freeTime >= 30*60) {
			breakTrips++
			continue
		} else {
			isAssigned := false
			for j:= 0;j<bucketCount;j++ {
				if uint8(cpy[j].clusterId) == nextTrip.pickupCluster {
					// Add to Bucket
					isAssigned = true
					nextTripTime := freeTime
					tripTimeForPredictionFollowers = append(tripTimeForPredictionFollowers, nextTripTime)
					store_resultsInTime(nextTrip.id, len(model.Centroids), fold, nextTrip.taxiId, 1, nextTripTime, j+1, db)
				}
			}
			if isAssigned == false {
				nextTripTime := freeTime
				tripTimeForPredictionNonFollowers = append(tripTimeForPredictionNonFollowers, nextTripTime)
				store_resultsInTime(nextTrip.id, len(model.Centroids), fold, nextTrip.taxiId, 0, nextTripTime, 0, db)
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
func train_pickupInTime(taxiIds []uint64, kMeansK int, kMeansMaxIteration int) ([]float64, *cluster.KMeans){
	var tripCollection []TripData
	tripCollection = loadTripsForTaxis(taxiIds)
	model := trainKMeans(tripCollection, kMeansK, kMeansMaxIteration)
	tripCollection = predictClusterAndAssign(tripCollection, model)
	frequencyMap := generateReverseClusterFrequencyMapInTime(tripCollection, kMeansK)
	rankedClusters := applyPageRank(frequencyMap, kMeansK)
	return rankedClusters, model
}

func generateReverseClusterFrequencyMapInTime(trips []TripData, kMeansK int) [][]float64 {
	var frequencyMap [][]uint64
	var costMap [][]big.Float
	var avgCostMap [][]float64

	for i:= 0; i < kMeansK; i++ {
		frequencyMap = append(frequencyMap,make([]uint64, kMeansK))
		costMap = append(costMap,make([]big.Float, kMeansK))
		avgCostMap = append(avgCostMap,make([]float64, kMeansK))
	}

	for _, trip := range trips {
		frequencyMap[trip.dropoffCluster][trip.pickupCluster]++
		costMap[trip.dropoffCluster][trip.pickupCluster].Add(&costMap[trip.dropoffCluster][trip.pickupCluster], new(big.Float).SetFloat64(trip.cost))
	}

	for i:= 0; i < kMeansK; i++ {
		for j:=0; j < kMeansK; j++ {
			if frequencyMap[i][j] > 0 {
				avgCostMap[i][j], _ = new(big.Float).Quo(&costMap[i][j], new(big.Float).SetUint64(frequencyMap[i][j])).Float64()
			}
		}
	}

	return avgCostMap
}

/**
Function is final and tested
 */
func getFoldTaxiIdsInTime(fold int, db *sql.DB) []uint64{

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
func getFoldTaxiNegateIdsInTime(fold int, db *sql.DB) []uint64{

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

func store_resultsInTime(tripId uint64, kMeansK int, fold int, taxi_id uint64, followed_prediction int, trip_time int, bucket int,  db *sql.DB) {
	query, err := db.Prepare("INSERT INTO results_pickup_pred(trip_id, k_means_k, fold, taxi_id, followed_prediction, trip_time, bucket, experiment) VALUES (?,?,?,?,?,?,?,?)")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	_, err = query.Exec(tripId, kMeansK, fold, taxi_id, followed_prediction, trip_time, bucket, 2)
	if err != nil {
		panic(err.Error())
	}

	defer query.Close()
}
