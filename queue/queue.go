package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/auth"
	"firebase.google.com/go/messaging"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/type/latlng"
)

var clientAuth *auth.Client
var clientFirestore *firestore.Client
var clientMessaging *messaging.Client
var clientHTTP = &http.Client{
	Timeout: 30 * time.Second,
}

var currentWorkOrder = WorkOrder{}

func init() {
	// Use the application default credentials
	ctx := context.Background()
	conf := &firebase.Config{DatabaseURL: "https://motoflash-a2f12.firebaseio.com/"}
	// Fetch the service account key JSON file contents
	opt := option.WithCredentialsFile("./motoflash-a2f12-500d186cdeb4.json")

	// Initialize the app with a service account, granting admin privileges
	app, err := firebase.NewApp(ctx, conf, opt)
	// conf := &firebase.Config{ProjectID: "motoflash-a2f12"}
	// app, err := firebase.NewApp(ctx, conf)
	if err != nil {
		log.Fatalf("error getting NewApp client: %v\n", err)
	}

	clientAuth, err = app.Auth(ctx)
	if err != nil {
		log.Fatalf("error getting Auth client: %v\n", err)
	}

	clientMessaging, err = app.Messaging(ctx)
	if err != nil {
		log.Fatalf("app.Messaging: %v", err)
		return
	}

	clientFirestore, err = app.Firestore(ctx)
	if err != nil {
		log.Fatalf("app.Firestore: %v", err)
		return
	}

}

func main() {
	http.HandleFunc("/", RunQueue)

	log.Println("Executando...")
	defer log.Println("Parando...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

//RunQueue -> Run the queue for the workorder
func RunQueue(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	// ctx, cancel := context.WithCancel(ctx)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Method not found %s", r.Method)
		return
	}

	tokenID := r.Header.Get("accesstoken")

	if len(tokenID) == 0 {
		apiKey := r.Header.Get("apikey")
		if apiKey != "&,+jQcPf4S#aoAyjC93Z990h6RKqRY" {
			showError(w, ErrorResponse{
				Message: "Unauthorized, invalid acessToken",
				Code:    401,
				Field:   "accesstoken",
				Type:    "UNAUTHORIZED",
			})
			return

		}
	} else {
		_, err := clientAuth.VerifyIDToken(ctx, tokenID)
		if err != nil {
			showError(w, ErrorResponse{
				Message: "Unauthorized, invalid acessToken",
				Code:    401,
				Field:   "accesstoken",
				Type:    "UNAUTHORIZED",
			})
			return
		}
	}

	path := strings.Split(r.URL.String(), "/")
	workOrderID := path[len(path)-1]

	ref := clientFirestore.Collection("workorders").Doc(workOrderID)
	c := make(chan ResultRunQueue)

	var couriers = []Courier{}

	go func(c chan ResultRunQueue) {
		var company Company = Company{}
		iter := ref.Snapshots(context.Background())
		for {
			doc, err := iter.Next()

			log.Println("start")

			if err != nil {
				log.Println(err)
				iter.Stop()
				return
			}

			if !doc.Exists() {
				iter.Stop()
				c <- ResultRunQueue{
					Error: &ErrorResponse{
						Code:    3,
						Message: "WorkOrderId not found",
						Type:    "NOT_EXIST",
						Field:   "workOrderId",
					},
				}
				return
			}

			var workOrder = WorkOrder{}
			doc.DataTo(&workOrder)

			log.Println("Point", workOrder.Points[0], workOder.Status)

			currentWorkOrder = workOrder

			if workOrder.Status == "ASSIGNED" {
				iter.Stop()
				c <- ResultRunQueue{
					WorkOrder: &workOrder,
				}
				return
			}

			if workOrder.Status == "CANCELLED" {
				iter.Stop()
				c <- ResultRunQueue{
					WorkOrder: &workOrder,
				}
				return
			}

			log.Println("companyID", workOrder.CompanyID)

			if len(workOrder.CompanyID) < 1 {
				iter.Stop()
				c <- ResultRunQueue{
					Error: &ErrorResponse{
						Code:    500,
						Message: "companyID empty",
						Type:    "INTERNAL",
						Field:   "companyID",
					},
				}
				return
			}

			if len(company.ID) < 1 {
				docCompany, err := clientFirestore.Collection("companies").Doc(workOrder.CompanyID).Get(ctx)
				if err != nil {
					log.Println(err)
				}
				docCompany.DataTo(&company)
				log.Println(docCompany.Data())
				log.Println(company)
			}

			if len(couriers) < 1 {
				requestBody, err := json.Marshal(map[string]interface{}{
					"motorcycle": workOrder.Motorcycle,
					"location": map[string]interface{}{
						"latitude":  workOrder.Points[0].Address.Location.Geopoint.Latitude,
						"longitude": workOrder.Points[0].Address.Location.Geopoint.Longitude,
					},
				})
				if err != nil {
					log.Println(err)
				}
				requestCouriers, err := http.NewRequest("POST", "https://dev-api-moto-flash.firebaseapp.com/couriers/nearby", bytes.NewBuffer(requestBody))
				requestCouriers.Header.Set("Content-type", "application/json")
				requestCouriers.Header.Set("key", "&,+jQcPf4S#aoAyjC93Z990h6RKqRY")

				resp, err := clientHTTP.Do(requestCouriers)

				if err != nil {
					log.Println(err)
				}

				defer resp.Body.Close()

				body := resp.Body
				json.NewDecoder(body).Decode(&couriers)

				log.Println("couriers:", len(couriers))

				if len(couriers) < 1 {
					c <- ResultRunQueue{
						Error: &ErrorResponse{
							Code:    3,
							Message: "Not found couriers",
							Type:    "NOT_EXIST",
							Field:   "couriers",
						},
					}
					return
				}
			}
			timeSleep := int64(0)

			if len(workOrder.Couriers) > 0 {
				diff := time.Now().Unix() - workOrder.Couriers[len(workOrder.Couriers)-1].SendPushDate
				timeSleep = company.AcceptTime - diff
				log.Println("diff", diff, "timeSleep", timeSleep)

				if workOrder.Couriers[len(workOrder.Couriers)-1].Status == "DENIED" || workOrder.Couriers[len(workOrder.Couriers)-1].Status == "NOT_SEND_PUSH" {
					timeSleep = 0
				}
			}

			if len(workOrder.Couriers) >= company.CourierCountPush || len(workOrder.Couriers) >= len(couriers) {
				log.Println("last courier", timeSleep)
				time.Sleep(time.Duration(timeSleep) * time.Millisecond)

				mapCouriers := []map[string]interface{}{}

				for _, value := range workOrder.Couriers {
					status := value.Status

					log.Println("value in couriers", value, currentWorkOrder.Couriers[len(workOrder.Couriers)-1].ID)
					if value.Status == "PENDING" && value.ID == currentWorkOrder.Couriers[len(workOrder.Couriers)-1].ID {
						status = "IGNORED"
					}
					mapCouriers = append(mapCouriers, map[string]interface{}{
						"id":           value.ID,
						"sendPushDate": value.SendPushDate,
						"status":       status,
					})
				}

				_, err = clientFirestore.Collection("workorders").Doc(workOrderID).Set(ctx, map[string]interface{}{
					"status":   "CANCELLED",
					"couriers": mapCouriers,
				}, firestore.MergeAll)
				if err != nil {
					c <- ResultRunQueue{
						Error: &ErrorResponse{
							Field:   "workOrder.status",
							Message: "Internal server error",
							Type:    "INTERNAL_ERROR",
						},
					}
					return
				}
				log.Println("last courier finish time")

			} else {
				go func() {
					log.Println("start go")

					courierToNotify := couriers[len(workOrder.Couriers)]
					var notify = true

					for _, value := range workOrder.Couriers {
						if value.ID == courierToNotify.ID {
							notify = false
						}
					}

					if notify {
						time.Sleep(time.Duration(timeSleep) * time.Millisecond)
						notifyCourier(courierToNotify, workOrderID, workOrder.Couriers, company.AcceptTime)
					} else {
						log.Println("not notify", courierToNotify.ID)
					}
					log.Println("finish go")
					return
				}()
			}

			log.Println("finish")
		}
	}(c)

	result := <-c
	if result.Error != nil {
		showError(w, *result.Error)
		return
	}
	responseBody, err := json.Marshal(result.WorkOrder)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error: %s", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
	return
}

func notifyCourier(courier Courier, workOrderID string, couriers []Courier, acceptTime int64) {
	if currentWorkOrder.Status != "PENDING" {
		log.Println("ignored push", courier.ID)
		return
	}

	for _, value := range currentWorkOrder.Couriers {
		if value.ID == courier.ID {
			log.Println("ignored push", courier.ID)
			return
		}
	}

	ctx := context.Background()
	log.Println("start send push to", courier.ID)

	var devicesTokens = []string{}

	iter := clientFirestore.Collection("couriers").Doc(courier.ID).Collection("devices").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println(err)
			return
		}
		var device = Device{}
		doc.DataTo(&device)
		devicesTokens = append(devicesTokens, device.DeviceToken)
	}

	message := &messaging.MulticastMessage{
		Data: map[string]string{
			"title":       "Test",
			"message":     "Message",
			"workOrderId": workOrderID,
			"acceptTime":  strconv.Itoa(int(acceptTime)),
			"distance":    strconv.FormatFloat(courier.HitMetadata.Distance, 'f', 6, 64),
		},
		Tokens: devicesTokens,
	}

	log.Println(devicesTokens)

	_, err := clientMessaging.SendMulticast(context.Background(), message)
	if err != nil {
		log.Println(err)
		couriers = append(couriers, Courier{
			ID:           courier.ID,
			SendPushDate: time.Now().Unix(),
			Status:       "NOT_SEND_PUSH",
		})
	} else {
		couriers = append(couriers, Courier{
			ID:           courier.ID,
			SendPushDate: time.Now().Unix(),
			Status:       "PENDING",
		})
	}
	mapCouriers := []map[string]interface{}{}

	for _, value := range couriers {
		status := value.Status
		if value.Status == "PENDING" && value.ID != courier.ID {
			status = "IGNORED"
		}
		mapCouriers = append(mapCouriers, map[string]interface{}{
			"id":           value.ID,
			"sendPushDate": value.SendPushDate,
			"status":       status,
		})
	}

	log.Println("updateCouriers", courier.ID)
	clientFirestore.Collection("workorders").Doc(workOrderID).Set(ctx, map[string]interface{}{
		"couriers": mapCouriers,
	}, firestore.MergeAll)
	log.Println("finish send push", courier.ID)
	return
}

func showError(w http.ResponseWriter, error ErrorResponse) {
	responseBody, err := json.Marshal(error)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error: %s", err)
		return
	}

	w.WriteHeader(http.StatusBadRequest)
	w.Write(responseBody)
}

type ErrorResponse struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
	Type    string `json:"type"`
	Field   string `json:"field"`
}

type ResultRunQueue struct {
	Error     *ErrorResponse `json:"message"`
	WorkOrder *WorkOrder     `json:"workOrder"`
}
type WorkOrder struct {
	UserID     string    `json:"userId"`
	CompanyID  string    `json:"companyId"`
	Motorcycle bool      `json:"motorcycle"`
	Couriers   []Courier `json:"couriers"`
	CourierID  string    `json:"courierId"`
	Quotation  struct {
		Price float64 `json:"price"`
		ID    string  `json:"id"`
	} `json:"quotation"`
	Status string `json:"status"`
	Points []struct {
		Address struct {
			Address1     string `json:"address1"`
			Address2     string `json:"address2"`
			Number       string `json:"number"`
			Neighborhood string `json:"neighborhood"`
			City         string `json:"city"`
			State        string `json:"state"`
			Location     struct {
				Geohash  string         `json:"geohash"`
				Geopoint *latlng.LatLng `json:"geopoint"`
			} `json:"location"`
		} `json:"address"`
		Sequence int    `json:"sequence"`
		Status   string `json:"status"`
		ID       string `json:"id"`
	} `json:"points"`
}

type Company struct {
	AcceptTime        int64   `json:"acceptTime"`
	CourierCountPush  int     `json:"courierCountPush"`
	ID                string  `json:"id"`
	Name              string  `json:"name"`
	PriceMin          float64 `json:"priceMin"`
	PricePerKm        float64 `json:"pricePerKm"`
	PricePerWorkOrder float64 `json:"pricePerWorkOrder"`
}

type Courier struct {
	ID               string      `json:"id"`
	CnhDoc           string      `json:"cnhDoc"`
	SendPushDate     int64       `json:"sendPushDate"`
	Status           string      `json:"status"`
	Online           bool        `json:"online"`
	Password         interface{} `json:"password"`
	Cnh              bool        `json:"cnh"`
	Active           bool        `json:"active"`
	Name             string      `json:"name"`
	Email            string      `json:"email"`
	CnhNumber        string      `json:"cnhNumber"`
	CurrentEquipment struct {
		Model       string `json:"model"`
		Plate       string `json:"plate"`
		Brand       string `json:"brand"`
		CreatedDate struct {
			Seconds     int `json:"_seconds"`
			Nanoseconds int `json:"_nanoseconds"`
		} `json:"createdDate"`
		Year int `json:"year"`
	} `json:"currentEquipment"`
	Location struct {
		Geopoint struct {
			Latitude  float64 `json:"_latitude"`
			Longitude int     `json:"_longitude"`
		} `json:"geopoint"`
		Geohash string `json:"geohash"`
	} `json:"location"`
	CreatedDate struct {
		Seconds     int `json:"_seconds"`
		Nanoseconds int `json:"_nanoseconds"`
	} `json:"createdDate"`
	Running     bool   `json:"running"`
	MobilePhone string `json:"mobilePhone"`
	HitMetadata struct {
		Distance float64 `json:"distance"`
		Bearing  float64 `json:"bearing"`
	} `json:"hitMetadata"`
}

type Device struct {
	DeviceToken string `json:"deviceToken"`
}

//gcloud functions deploy RunQueue --runtime go111 --trigger-http
