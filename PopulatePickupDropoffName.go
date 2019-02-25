package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
	"log"
	"encoding/json"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/remeh/sizedwaitgroup"
)

type NameOSRMReply struct {
	Waypoints []LocationName `json:waypoints`
}

type LocationName struct {
	Name     string  `json:name`
	Distance float32 `json:distance`
}

type Trip struct {
	id                     uint64
	plat, plng, dlat, dlng float64
	pname, dname           string
	pdistance, ddistance   float32
}

func main() {
	//log.Print(getLocationName(-8.618643, 41.141412))
	followPipeline()
}

func followPipeline() {
	db, err := sql.Open("mysql", "root:123@/thesis")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	swg := sizedwaitgroup.New(8)

	log.Print("Started")
	for i := 1700000; i <= 1800000; i++ {
		swg.Add()
		go cycle(i+1, db, &swg)
	}
	swg.Wait()
	log.Print("Completed")
}

func cycle(id int, db *sql.DB, swg *sizedwaitgroup.SizedWaitGroup) {
	t,_ := loadTrip(id, db)
	t.pname, t.pdistance,_ = getLocationName(t.plng, t.plat)
	t.dname, t.ddistance,_ = getLocationName(t.dlng, t.dlat)
	updateTrip(t, db)
	swg.Done()
}

func updateTrip(t Trip, db *sql.DB) {
	query, err := db.Prepare("UPDATE trip set pickup_name = ?, dropoff_name=?, pickup_distance=?, dropoff_distance=? where id = ?")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	_, err = query.Exec(t.pname, t.dname, t.pdistance, t.ddistance, t.id)
	if err != nil {
		log.Fatal(err)
	}
}

func loadTrip(id int, db *sql.DB) (Trip, bool) {
	var trip Trip;
	query, err := db.Prepare("SELECT t.id, t.lat_pickup, t.lat_dropoff, t.lng_pickup, t.lng_dropoff from trip t where t.id = ? and t.lat_pickup is not null")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	row := query.QueryRow(id)

	err = row.Scan(&trip.id, &trip.plat, &trip.dlat, &trip.plng, &trip.dlng)

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

func getLocationName(lng, lat float64) (string, float32, bool) {
	//url := "http://127.0.0.1:5000/route/v1/driving/%f,%f;%f,%f?steps=true"
	url := "http://127.0.0.1:5000/nearest/v1/driving/%f,%f?number=10"
	var osrmReply NameOSRMReply
	locationName := ""
	locationDistance := float32(0.0)

	url = fmt.Sprintf(url, lng, lat)
	response, err := http.Get(url)

	if err != nil {
		fmt.Printf("%s", err)
		return "-", 0.0, false
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Printf("%s", err)
		}
		json.Unmarshal(contents, &osrmReply)
	}

	for _, waypoint := range osrmReply.Waypoints {
		if (waypoint.Name != "") {
			locationName = waypoint.Name
			locationDistance = waypoint.Distance
			break;
		}
	}
	return locationName, locationDistance, false
}
