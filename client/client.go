package client

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type AzBlobstore struct {
	storageClient StorageClient
}

func New(storageClient StorageClient) (AzBlobstore, error) {
	return AzBlobstore{storageClient: storageClient}, nil
}

func (client *AzBlobstore) Put(sourceFilePath string, dest string) error {
	sourceMD5, err := client.getMD5(sourceFilePath)
	if err != nil {
		return err
	}

	source, err := os.Open(sourceFilePath)
	if err != nil {
		return err
	}

	defer source.Close() //nolint:errcheck

	md5, err := client.storageClient.Upload(source, dest)
	if err != nil {
		return fmt.Errorf("upload failure: %w", err)
	}

	if !bytes.Equal(sourceMD5, md5) {
		log.Println("The upload failed because of an MD5 inconsistency. Triggering blob deletion ...")

		err := client.storageClient.Delete(dest)
		if err != nil {
			log.Println(fmt.Errorf("blob deletion failed: %w", err))
		}

		return fmt.Errorf("the upload responded an MD5 %v does not match the source file MD5 %v", md5, sourceMD5)
	}

	log.Println("Successfully uploaded file")
	return nil
}

func (client *AzBlobstore) Get(source string, dest *os.File) error {

	return client.storageClient.Download(source, dest)
}

func (client *AzBlobstore) Delete(dest string) error {

	return client.storageClient.Delete(dest)
}

func (client *AzBlobstore) DeleteRecursive(prefix string) error {

	return client.storageClient.DeleteRecursive(prefix)
}

func (client *AzBlobstore) Exists(dest string) (bool, error) {

	return client.storageClient.Exists(dest)
}

func (client *AzBlobstore) Sign(dest string, action string, expiration time.Duration) (string, error) {
	action = strings.ToUpper(action)
	switch action {
	case "GET", "PUT":
		return client.storageClient.SignedUrl(action, dest, expiration)
	default:
		return "", fmt.Errorf("action not implemented: %s", action)
	}
}

func (client *AzBlobstore) getMD5(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	defer file.Close() //nolint:errcheck

	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate md5: %w", err)
	}

	return hash.Sum(nil), nil
}

func (client *AzBlobstore) List(prefix string) ([]string, error) {
	return client.storageClient.List(prefix)
}

func (client *AzBlobstore) Copy(srcBlob string, dstBlob string) error {

	return client.storageClient.Copy(srcBlob, dstBlob)
}
