package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azBlob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	azContainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"github.com/cloudfoundry/bosh-azure-storage-cli/config"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . StorageClient
type StorageClient interface {
	Upload(
		source io.ReadSeekCloser,
		dest string,
	) ([]byte, error)

	Download(
		source string,
		dest *os.File,
	) error

	Delete(
		dest string,
	) error

	Exists(
		dest string,
	) (bool, error)

	SignedUrl(
		requestType string,
		dest string,
		expiration time.Duration,
	) (string, error)

	List(
		prefix string,
	) ([]string, error)
}

type DefaultStorageClient struct {
	credential    *azblob.SharedKeyCredential
	serviceURL    string
	storageConfig config.AZStorageConfig
}

func NewStorageClient(storageConfig config.AZStorageConfig) (StorageClient, error) {
	credential, err := azblob.NewSharedKeyCredential(storageConfig.AccountName, storageConfig.AccountKey)
	if err != nil {
		return nil, err
	}

	serviceURL := fmt.Sprintf("https://%s.%s/%s", storageConfig.AccountName, storageConfig.StorageEndpoint(), storageConfig.ContainerName)

	return DefaultStorageClient{credential: credential, serviceURL: serviceURL, storageConfig: storageConfig}, nil
}

func (dsc DefaultStorageClient) Upload(
	source io.ReadSeekCloser,
	dest string,
) ([]byte, error) {
	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Uploading %s", blobURL)) //nolint:staticcheck
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return nil, err
	}

	uploadResponse, err := client.Upload(context.Background(), source, nil)
	return uploadResponse.ContentMD5, err
}

func (dsc DefaultStorageClient) Download(
	source string,
	dest *os.File,
) error {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, source)

	log.Println(fmt.Sprintf("Downloading %s", blobURL)) //nolint:staticcheck
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return err
	}

	blobSize, err := client.DownloadFile(context.Background(), dest, nil) //nolint:ineffassign,staticcheck
	if err != nil {
		return err
	}
	info, err := dest.Stat()
	if err != nil {
		return err
	}
	if blobSize != info.Size() {
		log.Printf("Truncating file according to the blob size %v", blobSize)
		dest.Truncate(blobSize) //nolint:errcheck
	}

	return nil
}

func (dsc DefaultStorageClient) Delete(
	dest string,
) error {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Deleting %s", blobURL)) //nolint:staticcheck
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return err
	}

	_, err = client.Delete(context.Background(), nil)

	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "RESPONSE 404") {
		return nil
	}

	return err
}

func (dsc DefaultStorageClient) Exists(
	dest string,
) (bool, error) {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Checking if blob: %s exists", blobURL)) //nolint:staticcheck
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return false, err
	}

	_, err = client.BlobClient().GetProperties(context.Background(), nil)
	if err == nil {
		log.Printf("File '%s' exists in bucket '%s'\n", dest, dsc.storageConfig.ContainerName)
		return true, nil
	}
	if strings.Contains(err.Error(), "RESPONSE 404") {
		log.Printf("File '%s' does not exist in bucket '%s'\n", dest, dsc.storageConfig.ContainerName)
		return false, nil
	}

	return false, err
}

func (dsc DefaultStorageClient) SignedUrl(
	requestType string,
	dest string,
	expiration time.Duration,
) (string, error) {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Getting signed url for blob %s", blobURL)) //nolint:staticcheck
	client, err := azBlob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return "", err
	}

	url, err := client.GetSASURL(sas.BlobPermissions{Read: true, Create: true}, time.Now().Add(expiration), nil)
	if err != nil {
		return "", err
	}

	// There could be occasional issues with the Azure Storage Account when requests hitting
	// the server are not responded to, and then BOSH hangs while expecting a reply from the server.
	// That's why we implement a server-side timeout here (30 mins for GET and 45 mins for PUT)
	// (see: https://learn.microsoft.com/en-us/rest/api/storageservices/setting-timeouts-for-blob-service-operations)
	if requestType == "GET" {
		url += "&timeout=1800"
	} else {
		url += "&timeout=2700"
	}

	return url, err
}

func (dsc DefaultStorageClient) List(
	prefix string,
) ([]string, error) {

	if prefix != "" {
		log.Println(fmt.Sprintf("Listing blobs in container %s with prefix '%s'", dsc.storageConfig.ContainerName, prefix)) //nolint:staticcheck
	} else {
		log.Println(fmt.Sprintf("Listing blobs in container %s", dsc.storageConfig.ContainerName)) //nolint:staticcheck
	}

	client, err := azContainer.NewClientWithSharedKeyCredential(dsc.serviceURL, dsc.credential, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create container client: %w", err)
	}

	options := &container.ListBlobsFlatOptions{}
	if prefix != "" {
		options.Prefix = &prefix
	}

	pager := client.NewListBlobsFlatPager(options)
	var blobs []string

	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("error retrieving page of blobs: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			blobs = append(blobs, *blob.Name)
		}
	}

	return blobs, nil
}
