package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type AzblobClient struct {
	serviceURL azblob.ServiceURL
}

func newClient() (AzblobClient, error) {
	accountName := os.Getenv("ACCOUNTNAME")
	accountKey := os.Getenv("ACCOUNTKEY")
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{Retry: azblob.RetryOptions{
		TryTimeout: time.Second * 5,
	}})

	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))

	return AzblobClient{serviceURL: azblob.NewServiceURL(*u, p)}, nil
}

func (c AzblobClient) getContainer(name string) (*azblob.ContainerItem, error) {
	ctx := context.Background()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listContainer, err := c.serviceURL.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})
		if err != nil {
			log.Fatal(err)
		}
		marker = listContainer.NextMarker
		for _, container := range listContainer.ContainerItems {
			if container.Name == name {
				return &container, nil
			}
		}
	}
	return nil, errors.New("container not found")
}

func (c AzblobClient) uploadBlob(containerName, blobName string, b json.RawMessage) error {
	containerURL := c.serviceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)
	ctx := context.Background()
	option := azblob.UploadToBlockBlobOptions{
		BlobHTTPHeaders:  azblob.BlobHTTPHeaders{ContentType: "application/json"},
		AccessConditions: azblob.BlobAccessConditions{ModifiedAccessConditions: azblob.ModifiedAccessConditions{IfNoneMatch: "*"}},
	}
	_, err := azblob.UploadBufferToBlockBlob(ctx, b, blobURL, option)
	if err != nil {
		//if container not found crate container
		return err
	}
	return nil
}

func (c AzblobClient) DownloadBlob(containerName, blobName string) (json.RawMessage, error) {
	containerURL := c.serviceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(blobName)
	ctx := context.Background()
	response, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Fatal(err)
	}
	downloadedData := &bytes.Buffer{}
	reader := response.Body(azblob.RetryReaderOptions{})
	downloadedData.ReadFrom(reader)
	reader.Close()
	return downloadedData.Bytes(), nil
}

func (c AzblobClient) createContainer(name string) error {
	containerURL := c.serviceURL.NewContainerURL(name)
	ctx := context.Background()
	_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	fmt.Println("main start")
	client, err := newClient()
	if err != nil {
		fmt.Println("error")
		fmt.Println(err)
		return
	}
	data, err := client.DownloadBlob("testcontainer", "2020-04-18T17:48:06+09:00.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("raw data: %q", data)
	blobName := "testblob" + time.Now().Format(time.RFC3339) + ".json"
	testData := map[string]int{"apple": 5, "lettuce": 7}
	testDataB, _ := json.Marshal(testData)
	if err := client.uploadBlob("testcontainer", blobName, testDataB); err != nil {
		fmt.Println("failed to upload")
		fmt.Println(err)
		return
	}
	fmt.Println("main finished")
}
