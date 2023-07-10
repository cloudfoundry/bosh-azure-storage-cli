package client

import (
	"context"
	"fmt"
	azBlob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/cloudfoundry/bosh-azure-storage-cli/config"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . StorageClient
type StorageClient interface {
	Upload(
		source io.ReadSeekCloser,
		dest string,
	) error

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
		dest string,
		expiration time.Duration,
	) (string, error)
}

type DefaultStorageClient struct {
	credential *azblob.SharedKeyCredential
	serviceURL string
	storageConfig config.AZStorageConfig
}

func NewStorageClient(storageConfig config.AZStorageConfig) (StorageClient, error) {
	credential, err := azblob.NewSharedKeyCredential(storageConfig.AccountName, storageConfig.AccountKey)
	if err != nil {
		return nil, err
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", storageConfig.AccountName, storageConfig.ContainerName)

	return DefaultStorageClient{credential: credential, serviceURL: serviceURL, storageConfig: storageConfig}, nil
}

func (dsc DefaultStorageClient) Upload(
	source io.ReadSeekCloser,
	dest string,
) error {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Uploading %s", blobURL))
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return err
	}

	_, err = client.Upload(context.Background(), source, nil)
	if err != nil {
		return fmt.Errorf("upload failure: %s", err.Error())
	}

	log.Println("Successfully uploaded file")
	return nil
}

func (dsc DefaultStorageClient) Download(
	source string,
	dest *os.File,
) error {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, source)

	log.Println(fmt.Sprintf("Downloading %s", blobURL))
	client, err := blockblob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return err
	}

	_, err = client.DownloadFile(context.Background(), dest, nil)

	return err
}

func (dsc DefaultStorageClient) Delete(
	dest string,
) error {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Deleting %s", blobURL))
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

	log.Println(fmt.Sprintf("Checking if blob: %s exists", blobURL))
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
	dest string,
	expiration time.Duration,
) (string, error) {

	blobURL := fmt.Sprintf("%s/%s", dsc.serviceURL, dest)

	log.Println(fmt.Sprintf("Getting signed url for blob %s", blobURL))
	client, err := azBlob.NewClientWithSharedKeyCredential(blobURL, dsc.credential, nil)
	if err != nil {
		return "", err
	}

	url, err := client.GetSASURL(sas.BlobPermissions{Read: true, Create: true}, time.Now(), time.Now().Add(expiration))
	if err != nil {
		return "", err
	}

	return url, err
}
