package main

import (
	"fmt"
	"os"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/interfaces"
)

func main() {

	client, err := makeLdClient()

	if err != nil {
		fmt.Println("Error creating client:", err)
		os.Exit(1)
	}

	flagKey := os.Getenv("APP_FLAG_KEY")
	context := ldcontext.NewBuilder("context-key-123abc").
		Name("Sandy").
		Build()

	result, err := client.BoolVariation(flagKey, context, false)
	if err != nil {
		fmt.Println("Error evaluating flag:", err)
		os.Exit(1)
	}

	fmt.Printf("Flag Key [%s] result: [%v]", flagKey, result)
}

// makeLdClient returns a LDClient
// if LD_BASE_URI is set for the local dev server, then we configure the client to use the local dev server
func makeLdClient() (*ldclient.LDClient, error) {
	sdkKey := os.Getenv("LD_SDK_KEY")
	if sdkKey == "" {
		fmt.Println("LD_SDK_KEY environment variable not set")
		os.Exit(1)
	}

	var client *ldclient.LDClient
	var err error
	baseUri := os.Getenv("LD_BASE_URI")
	if baseUri == "" {
		client, err = ldclient.MakeClient(sdkKey, 5*time.Second)
	} else {
		conf := ldclient.Config{
			ServiceEndpoints: interfaces.ServiceEndpoints{
				Streaming: baseUri,
				Polling:   baseUri,
				Events:    baseUri,
			},
		}
		client, err = ldclient.MakeCustomClient(sdkKey, conf, 5*time.Second)
	}
	return client, err
}
