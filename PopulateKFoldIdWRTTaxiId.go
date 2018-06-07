package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math/rand"
	"github.com/remeh/sizedwaitgroup"
)

func main2() {

	db, err := sql.Open("mysql", "root:123@/thesis")
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	taxiIds := getDistinctTaxiId(db)
	swg := sizedwaitgroup.New(8)
	for _,taxiId := range taxiIds {
		swg.Add()
		go updateTaxiIdFold(taxiId, db, &swg)
	}
	swg.Wait()
}


/**
Function is final and tested
 */
func updateTaxiIdFold(taxiId uint64, db *sql.DB, wg *sizedwaitgroup.SizedWaitGroup) {

	rand := rand.Intn(10)+1

	query, err := db.Prepare("UPDATE trip SET k_fold_driver_id = ? WHERE taxi_id = ?")
	if err != nil {
		panic(err.Error())
	}
	log.Println("Updating Cluster", rand, "For Taxi", taxiId)
	defer query.Close()

	_, err = query.Exec(rand, taxiId)
	if err != nil {
		log.Fatal(err)
	}
	wg.Done()
}

/**
Function is final and tested
 */
func getDistinctTaxiId(db *sql.DB) []uint64{

	var taxiIds []uint64

	query, err := db.Prepare("SELECT distinct taxi_id from trip t")
	if err != nil {
		panic(err.Error())
	}
	defer query.Close()

	rows, err := query.Query()
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