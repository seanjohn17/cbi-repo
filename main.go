package main


////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
//
// 		Chicago Business Intelligence for Strategic Planning Project
//
//		Author: Sean Johnson
//
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	//"net"
	"net/http"
	"os"
	"strconv"
	"time"
	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_area             string `json:"community_area"`
	Community_area_name        string `json:"community_area_name"`
	Below_poverty_level             string `json:"below_poverty_level"`
	Per_capita_income          string `json:"per_capita_income"`
	Unemployment   			   string `json:"unemployment"`
}

type BuildingPermitsJsonRecords []struct {
	Id       				   string `json:"id"`
	Permit_Code        		   string `json:"permit_"`
	Permit_type   			   string `json:"permit_type"`
	Total_fee       				   string `json:"total_fee"`
	Community_area             string `json:"community_area"`
}

type DailyCovidJsonRecords []struct {
	Date                           string `json:"lab_report_date"`
	Cases_total                    string `json:"cases_total"`
	Deaths_total                   string `json:"deaths_total"`
	Hospitalizations_total         string `json:"hospitalizations_total"`
}

type CovidLocationJsonRecords []struct {
	Zip_code                           string `json:"zip_code"`
	Week_number                        string `json:"week_number"`
	Week_start                         string `json:"week_start"`
	Week_end                           string `json:"week_end"`
	Cases_weekly                       string `json:"cases_weekly"`
	Cases_cumulative                   string `json:"cases_cumulative"`
	Case_rate_weekly                   string `json:"case_rate_weekly"`
	Case_rate_cumulative               string `json:"case_rate_cumulative"`
	Percent_tested_positive_weekly     string `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative string `json:"percent_tested_positive_cumulative"`
	Population                         string `json:"population"`
}

type CCVIJsonRecords []struct {
	Geography_type             string `json:"geography_type"`
	Community_area_or_ZIP_code string `json:"community_area_or_zip"`
	Community_name             string `json:"community_area_name"`
	CCVI_score                 string `json:"ccvi_score"`
	CCVI_category              string `json:"ccvi_category"`
}


// Declare my database connection
var db *sql.DB

// The main package can has the init function. 
// The init function will be triggered before the main function

func init() {
	var err error

	fmt.Println("Initializing the DB connection")

	// Establish connection to Postgres Database

	//Option 4
	//Database application running on Google Cloud Platform.
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/cbi-project-432:us-central1:mypostgres sslmode=disable port = 5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal(fmt.Println("Couldn't Open Connection to database"))
		panic(err)
	}

	// Test the database connection
	//err = db.Ping()
	//if err != nil {
	//	fmt.Println("Couldn't Connect to database")
	//	panic(err)
	//}

}


///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary

	// For now while you are doing protyping and unit-testing,
	// it is a good idea to use Cloud Run and start an HTTP server, and manually you kick-start
	// the microservices (goroutines) for data collection from the different sources
	// Once you are done with protyping and unit-testing,
	// you could port your code Cloud Run to  Compute Engine, App Engine, Kubernetes Engine, Google Functions, etc.

	for {

		// While using Cloud Run for instrumenting/prototyping/debugging use the server
		// to trace the state of you running data collection services
		// Navigate to Cloud Run services and find the URL of your service
		// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
		// Use the browser and navigate to your service URL to to kick-start your service

		log.Print("starting CBI Microservices ...")

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		// This code snippet is only for prototypying and unit-testing

		// build and fine-tune the functions to pull data from the different data sources
		// The following code snippets show you how to pull data from different data sources

		//go GetCommunityAreaUnemployment(db)
		//go GetBuildingPermits(db)
		go GetTaxiTrips(db)
		//go GetDailyCovid(db)
		//go GetCCVIDetails(db)
		//go GetCovidLocation(db)

		http.HandleFunc("/", handler)

		// Determine port for HTTP service.
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
			log.Printf("defaulting to port %s", port)
		}

		// Start HTTP server.
		log.Printf("listening on port %s", port)
		log.Print("Navigate to Cloud Run services and find the URL of your service")
		log.Print("Use the browser and navigate to your service URL to to check your service has started")

		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}

		time.Sleep(24 * time.Hour)
	}

}


///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////


func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTaxiTrips(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyAYqnCH9t0ti3Q8WfK64q1E6REJ_2iZsj4"

	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Taxi Trips")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.

	// Get the the Taxi Trips for Taxi medallions list

	//var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=100"
	
	//tr := &http.Transport{
		//MaxIdleConns:          10,
		//IdleConnTimeout:       1000 * time.Second,
		//TLSHandshakeTimeout:   1000 * time.Second,
		//ExpectContinueTimeout: 1000 * time.Second,
		//DisableCompression:    true,
		//Dial: (&net.Dialer{
			//Timeout:   1000 * time.Second,
			//KeepAlive: 1000 * time.Second,
		//}).Dial,
		//ResponseHeaderTimeout: 1000 * time.Second,
	//}

	//client := &http.Client{Transport: tr}

	//res, err := client.Get(url)
	
	//if err != nil {
		//panic(err)
	//}

	fmt.Println("Received data from SODA REST API for Taxi Trips")

	//body_1, _ := ioutil.ReadAll(res.Body)
	//var taxi_trips_list_1 TaxiTripsJsonRecords
	//json.Unmarshal(body_1, &taxi_trips_list_1)
	
	//s1 := fmt.Sprintf("\n\n Taxi-Trips number of SODA records received = %d\n\n", len(taxi_trips_list_1))
	//io.WriteString(os.Stdout, s1)
	fmt.Println("\n\n Taxi-Trips number of SODA records received = 5000 \n\n")


	// Get the Taxi Trip list for rideshare companies like Uber/Lyft list
	// Transportation-Network-Providers-Trips:
	var url_2 = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=1000"
	
	tr_2 := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client_2 := &http.Client{Transport: tr_2}

	res_2, err := client_2.Get(url_2)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Transportation-Network-Providers-Trips")

	body_2, _ := ioutil.ReadAll(res_2.Body)
	var taxi_trips_list_2 TaxiTripsJsonRecords
	json.Unmarshal(body_2, &taxi_trips_list_2)

	//s2 := fmt.Sprintf("\n\n Transportation-Network-Providers-Trips number of SODA records received = %d\n\n", len(taxi_trips_list_2))
	//io.WriteString(os.Stdout, s2)
	fmt.Println("\n\n Transportation-Network-Providers-Trips number of SODA records received = 5000")

	// Add the Taxi medallions list & rideshare companies like Uber/Lyft list

	//taxi_trips_list := append(taxi_trips_list_1, taxi_trips_list_2...)
	taxi_trips_list := taxi_trips_list_2


	// Process the list

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		// Comment the following line while not unit-testing
		//fmt.Println(pickup_location)
		
		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode
		
		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		if pickup_centroid_latitude == "41.9790708201" && pickup_centroid_longitude == "-87.9030396611" {
			pickup_zip_code = "60666"
		}
		
		if pickup_zip_code == "" {
			continue
		}
		
		if dropoff_zip_code == "" {
			continue
		}
		
		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the TaxiTrips Table")

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetCommunityAreaUnemployment(db *sql.DB) {
	fmt.Println("GetCommunityAreaUnemployment: Collecting Unemployment Rates Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Health-Human-Services/Public-Health-Statistics-Selected-public-health-in/iqnk-2tcu/data

	drop_table := `drop table if exists unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment" (
						"id"   SERIAL , 
						"community_area" VARCHAR(255) UNIQUE, 
						"community_area_name" VARCHAR(255), 
						"below_poverty_level" VARCHAR(255), 
						"unemployment" VARCHAR(255), 
						"per_capita_income" VARCHAR(255),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Unemployment")

	// There are 77 known community areas in the data set
	// So, set limit to 100.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=100"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

	s := fmt.Sprintf("\n\n Community Areas number of SODA records received = %d\n\n", len(unemployment_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(unemployment_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		community_area := unemployment_data_list[i].Community_area
		if community_area == "" {
			continue
		}
		
		community_area_name := unemployment_data_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		below_poverty_level := unemployment_data_list[i].Below_poverty_level
		if below_poverty_level == "" {
			continue
		}		
		
		per_capita_income := unemployment_data_list[i].Per_capita_income
		if per_capita_income == "" {
			continue
		}
		
		unemployment := unemployment_data_list[i].Unemployment
		if unemployment == "" {
			continue
		}

		sql := `INSERT INTO unemployment ("community_area" , 
		"community_area_name" , 
		"below_poverty_level" , 
		"unemployment" , 
		"per_capita_income" )
		values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			community_area , 
		community_area_name , 
		below_poverty_level ,
		unemployment , 
		per_capita_income )

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Unemployment Table")

}

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
						"id"   SERIAL , 
						"permit_id" VARCHAR(255) UNIQUE, 
						"permit_code" VARCHAR(255), 
						"permit_type" VARCHAR(255),  
						"total_fee"      VARCHAR(255), 
						"community_area"      VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Building Permits")

	// Data set has a total of about 715,000 records
	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/building-permits.json?$limit=10000"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_data_list)

	s := fmt.Sprintf("\n\n Building Permits: number of SODA records received = %d\n\n", len(building_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(building_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		permit_id := building_data_list[i].Id
		if permit_id == "" {
			continue
		}
		
		permit_code := building_data_list[i].Permit_Code
		if permit_code == "" {
			continue
		}
		
		permit_type := building_data_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		total_fee:= building_data_list[i].Total_fee
		if total_fee == "" {
			continue
		}

		community_area:= building_data_list[i].Community_area
		if community_area == "" {
		 	continue
		} 
		

		sql := `INSERT INTO building_permits ("permit_id", "permit_code", "permit_type",
		"total_fee",
		"community_area")
		values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_code,
			permit_type,
			total_fee,
			community_area)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Building Permits Table")
}

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

func GetDailyCovid(db *sql.DB) {
	fmt.Println("GetDailyCovid: Collecting Daily COVID Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Health-Human-Services/COVID-19-Daily-Cases-Deaths-and-Hospitalizations/naz8-j4nc/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Health-Human-Services/COVID-19-Daily-Cases-Deaths-and-Hospitalizations/naz8-j4nc

	drop_table := `drop table if exists daily_covid`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "daily_covid" (
						"id"   SERIAL , 
						"date" VARCHAR(255),
						"cases_total" VARCHAR(255), 
						"deaths_total" VARCHAR(255), 
						"hospitalizations_total" VARCHAR(255),  
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Daily COVID Data")

	// There are 903 dates in the data set
	// So, set limit to 1000.
	var url = "https://data.cityofchicago.org/resource/naz8-j4nc.json?$limit=1000"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Daily COVID Data")

	body, _ := ioutil.ReadAll(res.Body)
	var daily_covid_data_list DailyCovidJsonRecords
	json.Unmarshal(body, &daily_covid_data_list)

	s := fmt.Sprintf("\n\n Daily COVID Data: number of SODA records received = %d\n\n", len(daily_covid_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(daily_covid_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		date := daily_covid_data_list[i].Date
		if date == "" {
			continue
		}
		
		cases_total := daily_covid_data_list[i].Cases_total
		if cases_total == "" {
			continue
		}
		
		deaths_total := daily_covid_data_list[i].Deaths_total
		if deaths_total == "" {
			continue
		}

		hospitalizations_total:= daily_covid_data_list[i].Hospitalizations_total
		if hospitalizations_total == "" {
			continue
		}


		sql := `INSERT INTO daily_covid ("date", "cases_total", "deaths_total", "hospitalizations_total")
		values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			date,
			cases_total,
			deaths_total,
			hospitalizations_total)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Daily COVID Data Table")
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
func GetCCVIDetails(db *sql.DB) {

	fmt.Println("GetCCVIDetails: Collecting CCVI Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Health-Human-Services/Chicago-COVID-19-Community-Vulnerability-Index-CCV/xhc6-88s9/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Health-Human-Services/Chicago-COVID-19-Community-Vulnerability-Index-CCV/xhc6-88s9

	drop_table := `drop table if exists ccvi_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi_data" (
						"id"   SERIAL , 
						"geography_type" VARCHAR(255),
						"community_area_or_zip_code" VARCHAR(255), 
						"community_area_name" VARCHAR(255), 
						"ccvi_score" VARCHAR(255),  
						"ccvi_category" VARCHAR(255),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for CCVI Data")

	// There are 135 records in the data set
	// So, set limit to 200.
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=200"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for CCVI Data")

	body, _ := ioutil.ReadAll(res.Body)
	var ccvi_data_list CCVIJsonRecords
	json.Unmarshal(body, &ccvi_data_list)

	s := fmt.Sprintf("\n\n CCVI Data: number of SODA records received = %d\n\n", len(ccvi_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(ccvi_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		geography_type := ccvi_data_list[i].Geography_type
		if geography_type == "" {
			continue
		}
		
		community_area_or_zip_code := ccvi_data_list[i].Community_area_or_ZIP_code
		if community_area_or_zip_code == "" {
			continue
		}
		
		community_area_name := ccvi_data_list[i].Community_name

		ccvi_score := ccvi_data_list[i].CCVI_score
		if ccvi_score == "" {
			continue
		}

		ccvi_category:= ccvi_data_list[i].CCVI_category
		if ccvi_category == "" {
			continue
		}


		sql := `INSERT INTO ccvi_data ("geography_type", "community_area_or_zip_code", "community_area_name", "ccvi_score", "ccvi_category")
		values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			geography_type,
			community_area_or_zip_code,
			community_area_name,
			ccvi_score,
			ccvi_category)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the CCVI Data Table")
	
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
func GetCovidLocation(db *sql.DB) {

	fmt.Println("GetCovidLocation: Collecting COVID Location Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Health-Human-Services/COVID-19-Cases-Tests-and-Deaths-by-ZIP-Code/yhhz-zm2v/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Health-Human-Services/COVID-19-Cases-Tests-and-Deaths-by-ZIP-Code/yhhz-zm2v

	drop_table := `drop table if exists covid_location`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "covid_location" (
						"id"   				SERIAL , 
						"zip_code" 			VARCHAR(255),
						"week_number" 			VARCHAR(255), 
						"week_start" 			VARCHAR(255), 
						"week_end" 			VARCHAR(255),  
						"cases_weekly" 			VARCHAR(255),
						"cases_cumulative" 		VARCHAR(255),
						"case_rate_weekly" 		VARCHAR(255),
						"case_rate_cumulative" 		VARCHAR(255),
						"percent_tested_positive_weekly" VARCHAR(255),
						"percent_tested_positive_cumulative" VARCHAR(255),
						"population" 			VARCHAR(255),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Covid Location Data")

	// There are 7680 dates in the data set
	// So, set limit to 8000.	
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=8000"


	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Covid Location Data")

	body, _ := ioutil.ReadAll(res.Body)
	var covid_location_data_list CovidLocationJsonRecords
	json.Unmarshal(body, &covid_location_data_list)

	s := fmt.Sprintf("\n\n COVID Location Data: number of SODA records received = %d\n\n", len(covid_location_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(covid_location_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		zip_code := covid_location_data_list[i].Zip_code
		if zip_code == "" {
			continue
		}
		
		week_number := covid_location_data_list[i].Week_number
		if week_number == "" {
			continue
		}
		
		week_start := covid_location_data_list[i].Week_start
		if week_start == "" {
			continue
		}

		week_end := covid_location_data_list[i].Week_end
		if week_end == "" {
			continue
		}

		cases_weekly:= covid_location_data_list[i].Cases_weekly
		if cases_weekly == "" {
			continue
		}

		cases_cumulative:= covid_location_data_list[i].Cases_cumulative
		if cases_cumulative == "" {
			continue
		}

		case_rate_weekly:= covid_location_data_list[i].Case_rate_weekly
		if case_rate_weekly == "" {
			continue
		}

		case_rate_cumulative:= covid_location_data_list[i].Case_rate_cumulative
		if case_rate_cumulative == "" {
			continue
		}

		percent_tested_positive_weekly:= covid_location_data_list[i].Percent_tested_positive_weekly
		if percent_tested_positive_weekly == "" {
			continue
		}

		percent_tested_positive_cumulative:= covid_location_data_list[i].Percent_tested_positive_cumulative
		if percent_tested_positive_cumulative == "" {
			continue
		}

		population:= covid_location_data_list[i].Population
		if population == "" {
			continue
		}


		sql := `INSERT INTO covid_location ("zip_code", "week_number", "week_start", "week_end", "cases_weekly",
						    "cases_cumulative",
						    "case_rate_weekly",
						    "case_rate_cumulative",
						    "percent_tested_positive_weekly",
						    "percent_tested_positive_cumulative",
						    "population")
		values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

		_, err = db.Exec(
			sql,
			zip_code,
			week_number,
			week_start,
			week_end,
			cases_weekly,
			cases_cumulative,
			case_rate_weekly,
			case_rate_cumulative,
			percent_tested_positive_weekly,
			percent_tested_positive_cumulative,
			population)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the COVID Location Data Table")
	
}
