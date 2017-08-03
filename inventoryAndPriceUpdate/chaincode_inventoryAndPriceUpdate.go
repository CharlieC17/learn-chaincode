/*
Copyright IBM Corp. 2016 All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
		 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

type TablesChaincode struct {
	// Exported Type TablesChaincode
}

// Init method will be called during deployment.
func (t *TablesChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}
	return nil, nil
}

// Invoke: this method inserts rows to the table: 4 args expected (the column values)
func (t *TablesChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "createtables" {
		// Creates tables: InventoryHistory and PriceListHistory
		t.createInventoryTable(stub, args)
		//t.createPriceListTable(stub, args)
		return nil, nil
	} else if function == "invokeInventory" && len(args) == 3 {
		// invokeInventory: insert a record into InventoryHistory table
		return t.invokeInventory(stub, args)
	} else if function == "invokePriceList" && len(args) == 3 {
		// invokePriceList: insert a record into PriceListHistory table
		return t.invokePriceList(stub, args)
	}
	fmt.Printf("Invalid invocation of Invoke method")
	return nil, nil
}

// func to create the InventoryHistory table
func (t *TablesChaincode) createInventoryTable(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	/* Create table:
	Keys: ItemId, OrgCode, CreateTS
	Value: Qty for the above unique combination
	CreateTS: Stored as a string in the format 'YYYYMMDDhhmmss'
	*/
	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments for createtable method. Expecting zero args")
	}

	err := stub.CreateTable("InventoryHistory", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "ItemId", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "OrgCode", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "CreateTS", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "Qty", Type: shim.ColumnDefinition_INT32, Key: false},
	})
	if err != nil {
		return nil, errors.New("Failed creating InventoryHistory table")
	}

	return nil, nil
}

// func to create PriceListHistory table
func (t *TablesChaincode) createPriceListTable(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	/* Create table:
	Keys: ItemId, CreateTS
	Value: Price for the above unique combination
	CreateTS: Stored as a string in the format 'YYYYMMDDhhmmss'
	*/
	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments for createtable method. Expecting zero args")
	}

	err := stub.CreateTable("PriceListHistory", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "ItemId", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "OrgCode", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "CreateTS", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "Price", Type: shim.ColumnDefinition_INT32, Key: false},
	})
	if err != nil {
		return nil, errors.New("Failed creating PriceListHistory table")
	}

	return nil, nil
}

// func to create the InventoryHistory table
func (t *TablesChaincode) invokeInventory(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	// the column values to insert a row
	itemid := args[0]
	orgcode := args[1]
	qtyI, errConv := strconv.Atoi(args[2])
	qty := int32(qtyI)
	//qty, errConv := strconv.ParseInt(args[4], 10, 32) -> "converted into int64 and not int32"
	if errConv != nil {
		fmt.Println("error converting string to int32")
	}

	/* Get current timestamp - format: YYYYMMDDhhmmss*/
	timenow := time.Now()
	createts := timenow.Format("20060102150405")

	//fmt.Println("Inserting a record to the inventory history table: [%s],[%s],[%s],[%s],[%d]", itemid, orgcode, node, date, qty)
	fmt.Println("Inserting a record to the inventory history table: ", itemid, orgcode, createts, qty)

	ok, err := stub.InsertRow("InventoryHistory", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: itemid}},
			&shim.Column{Value: &shim.Column_String_{String_: orgcode}},
			&shim.Column{Value: &shim.Column_String_{String_: createts}},
			&shim.Column{Value: &shim.Column_Int32{Int32: qty}}},
	})

	if !ok && err == nil {
		return nil, errors.New("The record already exists")
	}

	return nil, nil
}

// func to create the InventoryHistory table
func (t *TablesChaincode) invokePriceList(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	// the column values to insert a row
	itemid := args[0]
	orgcode := args[1]
	priceI, errConv := strconv.Atoi(args[2])
	price := int32(priceI)
	//qty, errConv := strconv.ParseInt(args[4], 10, 32) -> "converted into int64 and not int32"
	if errConv != nil {
		fmt.Println("error converting string to int32")
	}

	/* Get current timestamp - format: YYYYMMDDhhmmss*/
	timenow := time.Now()
	createts := timenow.Format("20060102150405")

	//fmt.Println("Inserting a record to the inventory history table: [%s],[%s],[%s],[%s],[%d]", itemid, orgcode, node, date, qty)
	fmt.Println("Inserting a record to the inventory history table: ", itemid, orgcode, createts, price)

	ok, err := stub.InsertRow("InventoryHistory", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: itemid}},
			&shim.Column{Value: &shim.Column_String_{String_: orgcode}},
			&shim.Column{Value: &shim.Column_String_{String_: createts}},
			&shim.Column{Value: &shim.Column_Int32{Int32: price}}},
	})

	if !ok && err == nil {
		return nil, errors.New("The record already exists")
	}

	return nil, nil
}

// Query retrieves the records from the table
// 2 args expected: ItemId, OrgCode
func (t *TablesChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	var err error

	/* Preparing the key column array to query the table */
	var columns []shim.Column
	var model string

	if function == "inventory_ItemOrg" && len(args) == 2 {
		// table queried is InventoryHistory
		model = "InventoryHistory"

		/* The expected args: The keys ItemId and OrgCode */
		itemid := args[0]
		orgcode := args[1]

		col1 := shim.Column{Value: &shim.Column_String_{String_: itemid}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: orgcode}}

		/* append the columns required to be searched to the key column array */
		columns = append(columns, col1)
		columns = append(columns, col2)
	} else if function == "inventory_Item" && len(args) == 1 {
		// table queried is InventoryHistory
		model = "InventoryHistory"

		/* The expected args: The keys ItemId and OrgCode */
		itemid := args[0]

		col1 := shim.Column{Value: &shim.Column_String_{String_: itemid}}

		/* append the columns required to be searched to the key column array */
		columns = append(columns, col1)
	} else if function == "price_ItemOrg" && len(args) == 2 {
		// table queried is PriceListHistory
		model = "PriceListHistory"

		/* The expected args: The keys ItemId and OrgCode */
		itemid := args[0]
		orgcode := args[1]

		col1 := shim.Column{Value: &shim.Column_String_{String_: itemid}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: orgcode}}

		/* append the columns required to be searched to the key column array */
		columns = append(columns, col1)
		columns = append(columns, col2)
	} else if function == "price_Item" && len(args) == 1 {
		// table queried is PriceListHistory
		model = "PriceListHistory"

		/* The expected args: The keys ItemId and OrgCode */
		itemid := args[0]

		col1 := shim.Column{Value: &shim.Column_String_{String_: itemid}}

		/* append the columns required to be searched to the key column array */
		columns = append(columns, col1)
	} else {
		fmt.Printf("Invalid invocation of Query method")
	}

	/* Create a buffered channel var to store the rows returned by the GetRows function */
	var rowChannel <-chan shim.Row

	/* Query the InventoryHistory table to get the rows to the buffered channel */
	rowChannel, err = stub.GetRows(model, columns)
	if err != nil {
		return nil, err
	}

	var rows []*shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, &row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	// get length of the rows
	var rowslen int
	rowslen = len(rows)
	fmt.Println("Length of the rows is: ", rowslen)

	// loop through to get all the rows and respective columns:
	var outquery string
	var rowscnt int
	rowscnt = 0
	var colscnt int
	for rowscnt < rowslen {
		colscnt = 0
		outquery = outquery + "["
		for colscnt < 4 {
			if colscnt == 3 {
				currqty := int(rows[rowscnt].Columns[4].GetInt32())
				outquery = outquery + strconv.Itoa(currqty)
				fmt.Println("row[", rowscnt, "]col[", colscnt, "]", rows[rowscnt].Columns[4].GetInt32())
			} else {
				outquery = outquery + rows[rowscnt].Columns[colscnt].GetString_() + ","
				fmt.Println("row[", rowscnt, "]col[", colscnt, "]", rows[rowscnt].Columns[colscnt].GetString_())
			}
			colscnt = colscnt + 1
		}
		outquery = outquery + "]"
		rowscnt = rowscnt + 1
	}
	return []byte(fmt.Sprintf("Inventory history: {%s}", outquery)), nil
}

func main() {
	err := shim.Start(new(TablesChaincode))
	if err != nil {
		fmt.Printf("Error starting the chaincode: [%s]", err)
	}
}
