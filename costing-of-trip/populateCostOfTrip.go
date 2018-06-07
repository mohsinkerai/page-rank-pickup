package main

import (
	"encoding/json"
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"net/http"
	"fmt"
	"io/ioutil"
	_ "os"
	"github.com/remeh/sizedwaitgroup"
)

type Trip struct {
	id       uint64
	polyline string
}

type OSRMObject struct {
	Code   string             `json:code`
	Routes []DistanceDuration `json:routes`
}

type DistanceDuration struct {
	Distance float64 `json:"distance"`
	Time     float64 `json:"duration"`
}

func main() {
	load := 10000
	log.Println("Started Populating")

	db, err := sql.Open("mysql", "root:123@/thesis")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	swg := sizedwaitgroup.New(20)

	for i := 0; i < 180; i++ {
		swg.Add()
		go populate(i*load, load, db, &swg)
	}
	swg.Wait()
	log.Println("Completed Populating")
}

func populate(offset, load int, db *sql.DB, swg *sizedwaitgroup.SizedWaitGroup) {
	for i := offset; i < offset+load; i++ {
		trip, isLoaded := loadTrip(i, db)
		if isLoaded {
			distance, isCalculated := calculateDistanceOfTrip(trip)
			if isCalculated {
				updateDb(trip.id, distance, db)
			}
		}
	}
	swg.Done()
}

func updateDb(id uint64, distance float64, db *sql.DB) {
	query, err := db.Prepare("UPDATE  trip set trip_distance = ? where id = ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	_, err = query.Exec(distance, id)
	if err != nil {
		log.Fatal(err)
	}
}

/**
Loads a trip and return trip-data
 */
func loadTrip(id int, db *sql.DB) (Trip, bool) {
	var trip Trip;
	query, err := db.Prepare("SELECT t.id, polyline from trip t where t.id = ? and t.lat_pickup is not null and t.trip_distance is null")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	row := query.QueryRow(id)

	err = row.Scan(&trip.id, &trip.polyline)

	switch {
	case err == sql.ErrNoRows:
		log.Println("No Rows Found For id ", id)
		return trip, false
	case err != nil:
		log.Fatal(err)
		return trip, false
	default:
		return trip, true
	}
}

func calculateDistanceOfTrip(trip Trip) (float64, bool) {
	var polyline [][]float64
	var distance float64

	distance = 0.0
	polylineBytes := []byte(trip.polyline)
	if !json.Valid(polylineBytes) {
		log.Print("Invalid Json for CalculateDistanceOfTrip, Returning Zero")
		return 0.0, false
	}
	json.Unmarshal(polylineBytes, &polyline)
	totalWayPoints := len(polyline)

	if (totalWayPoints <= 0) {
		return 0.0, false
	}

	step := 10

	for index := 0; index < totalWayPoints; index += step {
		waypoint := polyline[index]
		if (index+step < totalWayPoints) {
			nextWaypoint := polyline[index+step]
			localDistance, _ := getDistance(waypoint[0], waypoint[1], nextWaypoint[0], nextWaypoint[1])
			distance = distance + localDistance
		} else {
			nextWaypoint := polyline[totalWayPoints-1]
			localDistance, _ := getDistance(waypoint[0], waypoint[1], nextWaypoint[0], nextWaypoint[1])
			distance = distance + localDistance
		}
	}

	return distance, true
}

func getDistance(lng, lat, lng2, lat2 float64) (float64, bool) {
	url := "http://127.0.0.1:5000/route/v1/driving/%f,%f;%f,%f?steps=true"
	var osrmReply OSRMObject

	url = fmt.Sprintf(url, lng, lat, lng2, lat2)
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("%s", err)
		return 0.0, false
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Printf("%s", err)
		}
		//isValid := json.Valid(contents)
		json.Unmarshal(contents, &osrmReply)
		//log.Println("json is valid", isValid)
		//fmt.Printf("%s\n", string(contents))
	}
	if len(osrmReply.Routes) < 1 {
		return 0.0, false
	}
	return osrmReply.Routes[0].Distance, true
}
