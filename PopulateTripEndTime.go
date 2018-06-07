package main

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"log"
	"encoding/json"
	"math"
	"github.com/remeh/sizedwaitgroup"
)

type TripBasic struct {
	id int
	polyline string
	tripInSeconds uint64
}

func main() {
	totalRows := 1710589
	rowsPerThread := 4000
	//totalRows := 41
	rowsCount := int(math.Ceil(float64(totalRows)/float64(rowsPerThread)))

	swg := sizedwaitgroup.New(25)

	for i:=0;i<rowsCount;i++ {
		swg.Add()
		go performGetAndUpdateRows(i, rowsPerThread, &swg)
	}
	swg.Wait()
}

func performGetAndUpdateRows(i, rowsPerThread int, wg *sizedwaitgroup.SizedWaitGroup) {
	db, err := sql.Open("mysql", "root:123@/thesis")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	log.Println("Performing Update for", i, rowsPerThread)
	trips := getTrips(i*rowsPerThread, (i+1)*rowsPerThread, db)
	for _, trip := range trips {
		var polyline [][]float64
		polylineBytes := []byte(trip.polyline)
		if !json.Valid(polylineBytes) {
			continue
		}
		json.Unmarshal(polylineBytes, &polyline)
		updateTripTime(trip.id, 15*len(polyline), db)
	}
	log.Println("Performed Update for", i, rowsPerThread)
	wg.Done()
}

func updateTripTime(tripId int, lengthInSeconds int, db *sql.DB) {

	query, err := db.Prepare("UPDATE  trip set trip_time_seconds = ? where id = ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	_, err = query.Exec(lengthInSeconds, tripId)
	if err != nil {
		log.Fatal(err)
	}
}

/**
Function is final and tested
 */
func getTrips(startId, endId int, db *sql.DB) []TripBasic{

	var trips []TripBasic

	query, err := db.Prepare("SELECT t.id, polyline from trip t where t.id >= ? and t.id < ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	rows, err := query.Query(startId, endId)
	if err != nil {
		panic (err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var tripBasic TripBasic
		err := rows.Scan(&tripBasic.id, &tripBasic.polyline)
		if err != nil {
			log.Println(rows)
			log.Fatal(err)
		}
		trips = append(trips, tripBasic)
	}
	return trips;
}
