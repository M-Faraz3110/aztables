package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
)

type InventoryEntity struct {
	aztables.Entity
	Price       float32
	Inventory   int32
	ProductName string
	OnSale      bool
}

type PurchasedEntity struct {
	aztables.Entity
	Price       float32
	ProductName string
	OnSale      bool
}

func getClient() *aztables.Client {
	accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT") //export AZURE_STORAGE_ACCOUNT=gostorageaccount123   export AZURE_TABLE_NAME=gostoragetable
	if !ok {
		panic("AZURE_STORAGE_ACCOUNT environment variable not found")
	}

	tableName, ok := os.LookupEnv("AZURE_TABLE_NAME")
	if !ok {
		panic("AZURE_TABLE_NAME environment variable not found")
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		panic(err)
	}
	serviceURL := fmt.Sprintf("https://%s.table.core.windows.net/%s", accountName, tableName)
	client, err := aztables.NewClient(serviceURL, cred, nil)
	if err != nil {
		panic(err)
	}
	return client
}

func createTable(client *aztables.Client) {
	//TODO: Check access policy, Storage Blob Data Contributor role needed
	_, err := client.CreateTable(context.TODO(), nil)
	if err != nil {
		panic(err)
	}
}

func addEntity(client *aztables.Client) {
	myEntity := InventoryEntity{
		Entity: aztables.Entity{
			PartitionKey: "pk001",
			RowKey:       "rk001",
		},
		Price:       3.99,
		Inventory:   20,
		ProductName: "Markers",
		OnSale:      false,
	}

	marshalled, err := json.Marshal(myEntity)
	if err != nil {
		panic(err)
	}

	_, err = client.AddEntity(context.TODO(), marshalled, nil) // TODO: Check access policy, need Storage Table Data Contributor role
	if err != nil {
		panic(err)
	}
}

func listEntities(client *aztables.Client) {
	listPager := client.NewListEntitiesPager(nil)
	pageCount := 0
	for listPager.More() {
		response, err := listPager.NextPage(context.TODO())
		if err != nil {
			panic(err)
		}
		fmt.Printf("There are %d entities in page #%d\n", len(response.Entities), pageCount)
		pageCount += 1
	}
}

func queryEntity(client *aztables.Client) {
	filter := fmt.Sprintf("PartitionKey eq '%v' or RowKey eq '%v'", "pk001", "rk001")
	top := int32(15)
	selectstr := "RowKey,Price,Inventory,ProductName,OnSale"
	p1 := &top
	p2 := &selectstr
	options := &aztables.ListEntitiesOptions{
		Filter: &filter,
		Select: p2,
		Top:    p1,
	}

	pager := client.NewListEntitiesPager(options)
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			panic(err)
		}
		for _, entity := range resp.Entities {
			var myEntity PurchasedEntity
			err = json.Unmarshal(entity, &myEntity)
			if err != nil {
				panic(err)
			}
			fmt.Println("Return custom type [PurchasedEntity]")
			fmt.Printf("Price: %v; ProductName: %v; OnSale: %v\n", myEntity.Price, myEntity.ProductName, myEntity.OnSale)
		}
	}
}

func deleteEntity(client *aztables.Client) {
	_, err := client.DeleteEntity(context.TODO(), "pk001", "rk001", nil)
	if err != nil {
		panic(err)
	}
}

func deleteTable(client *aztables.Client) {
	_, err := client.Delete(context.TODO(), nil)
	if err != nil {
		panic(err)
	}
}

func main() {

	fmt.Println("Authenticating...")
	client := getClient()

	fmt.Println("Creating a table...")
	createTable(client)

	fmt.Println("Adding an entity to the table...")
	addEntity(client)

	fmt.Println("Calculating all entities in the table...")
	listEntities(client)

	fmt.Println("Querying a specific entity...")
	queryEntity(client)

	// fmt.Println("Deleting an entity...")
	// deleteEntity(client)

	// fmt.Println("Deleting a table...")
	// deleteTable(client)
}
