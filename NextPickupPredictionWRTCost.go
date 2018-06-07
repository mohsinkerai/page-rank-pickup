package main

import (
	"log"
	"database/sql"
	"github.com/cdipaolo/goml/cluster"
	"sort"
	"github.com/remeh/sizedwaitgroup"
)

func main() {
	maxFold := 10
	//fold := 1
	kMeansK := 50
	kMeansMaxIteration := 1000
	experiment := 7
	bucketCount:= 5

	log.Println("kMeansK", kMeansK, "kMeansMaxIteration", kMeansMaxIteration, "bucketCount", bucketCount)

	swg := sizedwaitgroup.New(5)

	for fold:=0;fold<maxFold;fold++ {
		db, err := sql.Open("mysql", "root:123@/thesis")
		if err != nil {
			panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
		}
		defer db.Close()
		swg.Add()
		go runLogicForPickupInTime2(fold+1, db, kMeansK, kMeansMaxIteration,bucketCount,&swg,maxFold, experiment)
	}
	swg.Wait()

}

func runLogicForPickupInTime2(fold int, db *sql.DB, kMeansK int, kMeansMaxIteration int, bucketCount int, wg *sizedwaitgroup.SizedWaitGroup, totalFold int, experiment int) {
	taxiIds := getFoldTaxiNegateIdsInTime2(fold, totalFold, db)
	testTaxiIds := getFoldTaxiIdsInTime2(fold, db)
	rankedClusters, model := train_pickupInTime2(taxiIds, kMeansK, kMeansMaxIteration)
	log.Println("Training Completed for Fold", fold)
	clusterRank := convertRank(rankedClusters, kMeansK)
	continueCount, breakTrips, tripTimeForPredictionFollowers, tripTimeForPredictionNonFollowers := test_pickupInTime2(testTaxiIds, model, clusterRank, bucketCount, fold, db, experiment)
	log.Println("ContinueCount ", continueCount, "breakTrips", breakTrips, "Positive", average(tripTimeForPredictionFollowers), "+veTotal", len(tripTimeForPredictionFollowers), "Negative", average(tripTimeForPredictionNonFollowers), "-veTotal", len(tripTimeForPredictionNonFollowers))
	wg.Done()
}

/**
Training Model with Reverse Frequency Map
 */
func test_pickupInTime2(taxiIds []uint64, model *cluster.KMeans, rankedClusters []ClusterRank, bucketCount int, fold int, db *sql.DB, experiment int) (int, int, []int, []int){
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

		if currentTrip.dropoffTime.After(nextTrip.pickupTime) {
			continueCount++
			continue
		}

		freeTime := int(nextTrip.pickupTime.Sub(currentTrip.dropoffTime).Seconds());
		tripTime := int(nextTrip.dropoffTime.Sub(nextTrip.pickupTime).Seconds());

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
					tripTimeForPredictionFollowers = append(tripTimeForPredictionFollowers, tripTime)
					store_resultsInTime2(nextTrip.id, len(model.Centroids), fold, nextTrip.taxiId, 1, j+1, db, experiment, nextTrip.cost, tripTime, freeTime)
				}
			}
			if isAssigned == false {
				tripTimeForPredictionNonFollowers = append(tripTimeForPredictionNonFollowers, tripTime)
				store_resultsInTime2(nextTrip.id, len(model.Centroids), fold, nextTrip.taxiId, 0, 0, db, experiment, nextTrip.cost, tripTime, freeTime)
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
func train_pickupInTime2(taxiIds []uint64, kMeansK int, kMeansMaxIteration int) ([]float64, *cluster.KMeans){
	var tripCollection []TripData
	tripCollection = loadTripsForTaxis(taxiIds)
	model := trainKMeans(tripCollection, kMeansK, kMeansMaxIteration)
	tripCollection = predictClusterAndAssign(tripCollection, model)
	frequencyMap := generateReverseClusterFrequencyMapInTime(tripCollection, kMeansK)
	rankedClusters := applyPageRank(frequencyMap, kMeansK)
	return rankedClusters, model
}

/**
Function is final and tested
 */
func getFoldTaxiIdsInTime2(fold int, db *sql.DB) []uint64{

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
func getFoldTaxiNegateIdsInTime2(fold int, totalFold int, db *sql.DB) []uint64{

	var taxiIds []uint64

	query, err := db.Prepare("SELECT distinct t.taxi_id from trip t where k_fold_driver_id != ? and k_fold_driver_id <= ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	rows, err := query.Query(fold, totalFold)
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

func store_resultsInTime2(tripId uint64, kMeansK int, fold int, taxi_id uint64, followed_prediction int, bucket int,  db *sql.DB, experiment int, cost float64 , trip_time int, between_trip_time int) {
	query, err := db.Prepare("INSERT INTO results_pickup_pred_copy(trip_id, k_means_k, fold, taxi_id, followed_prediction, bucket, experiment, cost_earned, trip_time, between_trip_time) VALUES (?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		log.Fatalln("Unable to Write Results", err)
		panic(err.Error())
	}
	defer query.Close()

	_, err = query.Exec(tripId, kMeansK, fold, taxi_id, followed_prediction, bucket, experiment, cost, trip_time, between_trip_time)
	if err != nil {
		panic(err.Error())
	}

	defer query.Close()
}
